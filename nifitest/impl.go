package nifitest

import (
	"github.com/konpyutaika/nigoapi/pkg/nifi"
	"github.com/antihax/optional"
	"go.uber.org/zap"

	"bufio"
	"os"

	"time"
	"fmt"
	"context"
	"errors"
	"net/url"
	"net/http"
	"crypto/tls"
	"crypto/rand"
)

type testerImpl struct {
	log    *zap.Logger
	ctx    context.Context
	client *nifi.APIClient
}

func New(cfg *Config) (Tester, error) {
	clnt, err := getClient(cfg.URL, cfg.Proxy, cfg.VerifyServerCertificate)
	if err != nil {
		return nil, err
	}
	log, err := zap.NewDevelopment()
	if err != nil {
		return nil, err
	}
	ctx := context.TODO()
	token, err := getToken(ctx, clnt, cfg, log)
	if err != nil {
		return nil, err
	}
	ctx = context.WithValue(ctx, nifi.ContextAccessToken, token)
	return &testerImpl{
		log:    log,
		ctx:    ctx,
		client: clnt,
	}, nil
}

func (t *testerImpl) Run(flowId string,
	testData, expected map[Id]string) error {

	// get the proc
	pgfe, h, body, err := t.client.FlowApi.GetFlow(
		t.ctx,
		flowId,
		&nifi.FlowApiGetFlowOpts{
			UiOnly: optional.NewBool(false),
		})
	err = t.handleErr(err, h, body, 200, "GetFlow")
	if err != nil {
		return err
	}

	conns, h, body, err := t.client.ProcessGroupsApi.GetConnections(
		t.ctx,
		pgfe.ProcessGroupFlow.Id)
	err = t.handleErr(err, h, body, 200, "GetConnections")
	if err != nil {
		return err
	}

	injectors := make(map[Id]*nifi.ProcessorEntity)
	srcAdd := make(map[Id][]*nifi.ConnectionEntity)
	srcDel := make(map[Id][]*nifi.ConnectionEntity)
	sinks := make(map[Id]*nifi.ProcessorEntity)
	sinkAdd := make(map[Id][]*nifi.ConnectionEntity)
	sinkDel := make(map[Id][]*nifi.ConnectionEntity)

	for id, toInject := range testData {
		outgoing := connsOutOfId(id, conns.Connections)
		// creates an injector
		// deletes all outgoing connections from id
		// creates connections from the injector to the destinations
		// of the former outgoing connections of id6
		injector, addConns, delConns, err :=
			t.createInjector(&pgfe, outgoing, id, toInject)
		if injector != nil {
			injectors[id] = injector
		}
		if addConns != nil {
			srcAdd[id] = addConns
		}
		if delConns != nil {
			srcDel[id] = delConns
		}
		if err != nil {
			return t.rollback(&pgfe, injectors, srcAdd, srcDel,
				sinks, sinkAdd, sinkDel, err)
		}
	}

	for id, _ := range expected {
		incoming := connsIntoId(id, conns.Connections)
		// creates a sink
		// deletes all incoming connections into id
		// creates connections from the sources of the former incoming,
		// connections to the sink
		sink, addConns, delConns, err :=
			t.createSink(&pgfe, incoming, id)
		if sink != nil {
			sinks[id] = sink
		}
		if addConns != nil {
			sinkAdd[id] = addConns
		}
		if delConns != nil {
			sinkDel[id] = delConns
		}
		if err != nil {
			return t.rollback(&pgfe, injectors, srcAdd, srcDel,
				sinks, sinkAdd, sinkDel, err)
		}
	}

	reader := bufio.NewReader(os.Stdin)
	reader.ReadString('\n')

	err = t.startFlow(&pgfe, injectors, sinks)

	return t.rollback(&pgfe, injectors, srcAdd, srcDel, sinks, sinkAdd,
		sinkDel, err)
}

func (t *testerImpl) startFlow(pgfe *nifi.ProcessGroupFlowEntity,
	injectors, sinks map[Id]*nifi.ProcessorEntity) error {

	return nil
}

func (t *testerImpl) createInjector(pgfe *nifi.ProcessGroupFlowEntity,
	out []*nifi.ConnectionEntity, id Id, toInject string) (
	injector *nifi.ProcessorEntity,
	connected, disconnected []*nifi.ConnectionEntity, err error) {

	injector, err = t.doCreateInjector(pgfe, id.Id(), toInject)
	if err != nil {
		return nil, nil, nil, err
	}

	disconnected, err = t.deleteConns(out)
	if err != nil {
		return injector, nil, disconnected, err
	}

	connected, err = t.connectInjector(pgfe, injector, disconnected)
	if err != nil {
		return injector, connected, disconnected, err
	}
	return injector, connected, disconnected, nil
}

func (t *testerImpl) doCreateInjector(pgfe *nifi.ProcessGroupFlowEntity,
	id, toInject string) (*nifi.ProcessorEntity, error) {

	var version int64 = 0
	injector, h, body, err := t.client.ProcessGroupsApi.CreateProcessor(
		t.ctx,
		pgfe.ProcessGroupFlow.Id,
		nifi.ProcessorEntity{
			Revision: &nifi.RevisionDto{
				Version: &version,
			},
			DisconnectedNodeAcknowledged: false,
			Component: &nifi.ProcessorDto{
				// Bundle: nil,
				Name:  "Injector_" + id,
				Type_: "org.apache.nifi.processors.standard.GenerateFlowFile",
			},
		})
	err = t.handleErr(err, h, body, 201, "CreateProcessor")
	if err != nil {
		return nil, err
	}

	injector, h, body, err = t.client.ProcessorsApi.UpdateProcessor(
		t.ctx,
		injector.Id,
		nifi.ProcessorEntity{
			Revision: injector.Revision,
			DisconnectedNodeAcknowledged: false,
			Component: &nifi.ProcessorDto{
				Id: injector.Id,
				Name: "Injector_" + id,
				State: "STOPPED",
				Config: &nifi.ProcessorConfigDto{
					AutoTerminatedRelationships: []string{"success"},
					BulletinLevel: "WARN",
					ConcurrentlySchedulableTaskCount: 1,
					ExecutionNode: "ALL",
					PenaltyDuration: "30 sec",
					Properties: map[string]string{
						"generate-ff-custom-text": toInject,
					},
					RunDurationMillis: 0,
					SchedulingPeriod: "1 min",
					SchedulingStrategy: "TIMER_DRIVEN",
					YieldDuration: "1 sec",
				},
			},
		})
	err = t.handleErr(err, h, body, 200, "UpdateProcessor")
	if err != nil {
		return nil, err
	}

	return &injector, err
}

func (t *testerImpl) connectInjector(pgfe *nifi.ProcessGroupFlowEntity,
	injector *nifi.ProcessorEntity, disconn []*nifi.ConnectionEntity) (
	[]*nifi.ConnectionEntity, error) {

	conns := []*nifi.ConnectionEntity{}
	for _, conn := range disconn {
		c, err := t.doConnectInjector(pgfe, injector, conn)
		if err != nil {
			return conns, err
		}
		conns = append(conns, c)
	}
	return conns, nil
}

func (t *testerImpl) doConnectInjector(pgfe *nifi.ProcessGroupFlowEntity,
	src *nifi.ProcessorEntity, dst *nifi.ConnectionEntity) (
	*nifi.ConnectionEntity, error) {

	var version int64 = 0
	conn, h, body, err := t.client.ProcessGroupsApi.CreateConnection(
		t.ctx,
		pgfe.ProcessGroupFlow.Id,
		nifi.ConnectionEntity{
			Revision: &nifi.RevisionDto{
				Version: &version,
			},
			DisconnectedNodeAcknowledged: false,
			Component: &nifi.ConnectionDto{
				Source: &nifi.ConnectableDto{
					GroupId: pgfe.ProcessGroupFlow.Id,
					Id: src.Id,
					Type_: "PROCESSOR",
				},
				Destination: &nifi.ConnectableDto{
					GroupId: dst.DestinationGroupId,
					Id: dst.DestinationId,
					Type_: dst.DestinationType,
				},
				BackPressureDataSizeThreshold: "1 GB",
				BackPressureObjectThreshold: 10000,
				FlowFileExpiration: "0 sec",
				LoadBalanceCompression: "DO_NOT_COMPRESS",
				LoadBalancePartitionAttribute: "",
				LoadBalanceStrategy: "DO_NOT_LOAD_BALANCE",
				Name: "",
				Prioritizers: []string{},
				SelectedRelationships: []string{"success"},
			},
		})
	err = t.handleErr(err, h, body, 201, "CreateConnection")
	if err != nil {
		return nil, err
	}
	t.log.Debug("Connected " + src.Id + " -> " + dst.DestinationId)

	return &conn, nil
}

func (t *testerImpl) createSink(pgfe *nifi.ProcessGroupFlowEntity,
	in []*nifi.ConnectionEntity, id Id) (
	sink *nifi.ProcessorEntity,
	connected, disconnected []*nifi.ConnectionEntity, err error) {

	sink, err = t.doCreateSink(pgfe, id.Id())
	if err != nil {
		return nil, nil, nil, err
	}

	disconnected, err = t.deleteConns(in)
	if err != nil {
		return sink, nil, disconnected, err
	}

	connected, err = t.connectSink(pgfe, disconnected, sink)
	if err != nil {
		return sink, connected, disconnected, err
	}
	return sink, connected, disconnected, nil
}

func (t *testerImpl) doCreateSink(pgfe *nifi.ProcessGroupFlowEntity,
	id string) (*nifi.ProcessorEntity, error) {

	var version int64 = 0
	sink, h, body, err := t.client.ProcessGroupsApi.CreateProcessor(
		t.ctx,
		pgfe.ProcessGroupFlow.Id,
		nifi.ProcessorEntity{
			Revision: &nifi.RevisionDto{
				Version: &version,
			},
			DisconnectedNodeAcknowledged: false,
			Component: &nifi.ProcessorDto{
				// Bundle: nil,
				Name:  "Sink_" + id,
				Type_: "org.apache.nifi.processors.standard.PutFile",
				Config: &nifi.ProcessorConfigDto{
					AutoTerminatedRelationships: []string{
						"success",
						"failure",
					},
					BulletinLevel: "WARN",
					ConcurrentlySchedulableTaskCount: 1,
					ExecutionNode: "ALL",
					PenaltyDuration: "30 sec",
					Properties: map[string]string{
						"Directory": "test_" + id,
					},
					RunDurationMillis: 0,
					SchedulingPeriod: "1 min",
					SchedulingStrategy: "TIMER_DRIVEN",
					YieldDuration: "1 sec",
				},
			},
		})
	err = t.handleErr(err, h, body, 201, "CreateProcessor")
	if err != nil {
		return nil, err
	}

	sink, h, body, err = t.client.ProcessorsApi.UpdateProcessor(
		t.ctx,
		sink.Id,
		nifi.ProcessorEntity{
			Revision: sink.Revision,
			DisconnectedNodeAcknowledged: false,
			Component: &nifi.ProcessorDto{
				Id: sink.Id,
				Name: "Sink_" + id,
				State: "STOPPED",
				Config: &nifi.ProcessorConfigDto{
					AutoTerminatedRelationships: []string{
						"success",
						"failure",
					},
					BulletinLevel: "WARN",
					ConcurrentlySchedulableTaskCount: 1,
					ExecutionNode: "ALL",
					PenaltyDuration: "30 sec",
					Properties: map[string]string{
					},
					RunDurationMillis: 0,
					SchedulingPeriod: "1 min",
					SchedulingStrategy: "TIMER_DRIVEN",
					YieldDuration: "1 sec",
				},
			},
		})
	err = t.handleErr(err, h, body, 200, "UpdateProcessor")
	if err != nil {
		return nil, err
	}

	return &sink, err
}

func (t *testerImpl) connectSink(pgfe *nifi.ProcessGroupFlowEntity,
	disconn []*nifi.ConnectionEntity, sink *nifi.ProcessorEntity) (
	[]*nifi.ConnectionEntity, error) {

	conns := []*nifi.ConnectionEntity{}
	for _, conn := range disconn {
		c, err := t.doConnectSink(pgfe, conn, sink)
		if err != nil {
			return conns, err
		}
		conns = append(conns, c)
	}
	return conns, nil
}

func (t *testerImpl) doConnectSink(pgfe *nifi.ProcessGroupFlowEntity,
	src *nifi.ConnectionEntity, dst *nifi.ProcessorEntity) (
	*nifi.ConnectionEntity, error) {

	var version int64 = 0
	conn, h, body, err := t.client.ProcessGroupsApi.CreateConnection(
		t.ctx,
		pgfe.ProcessGroupFlow.Id,
		nifi.ConnectionEntity{
			Revision: &nifi.RevisionDto{
				Version: &version,
			},
			DisconnectedNodeAcknowledged: false,
			Component: &nifi.ConnectionDto{
				Source: &nifi.ConnectableDto{
					GroupId: src.SourceGroupId,
					Id: src.SourceId,
					Type_: src.SourceType,
				},
				Destination: &nifi.ConnectableDto{
					GroupId: pgfe.ProcessGroupFlow.Id,
					Id: dst.Id,
					Type_: "PROCESSOR",
				},
				BackPressureDataSizeThreshold: "1 GB",
				BackPressureObjectThreshold: 10000,
				FlowFileExpiration: "0 sec",
				LoadBalanceCompression: "DO_NOT_COMPRESS",
				LoadBalancePartitionAttribute: "",
				LoadBalanceStrategy: "DO_NOT_LOAD_BALANCE",
				Name: "",
				Prioritizers: []string{},
				SelectedRelationships: []string{"success", "failure"},
			},
		})
	err = t.handleErr(err, h, body, 201, "CreateConnection")
	if err != nil {
		return nil, err
	}
	t.log.Debug("Connected " + src.SourceId + " -> " + dst.Id)

	return &conn, nil
}

func (t *testerImpl) rollback(pgfe *nifi.ProcessGroupFlowEntity,
	injectors map[Id]*nifi.ProcessorEntity,
	injAdd, injDel map[Id][]*nifi.ConnectionEntity,
	sinks map[Id]*nifi.ProcessorEntity,
	sinkAdd, sinkDel map[Id][]*nifi.ConnectionEntity, err error) error {

	for id, injector := range injectors {
		e := t.rollbackNode(pgfe, injector, injAdd[id], injDel[id])
		if e != nil {
			err = chainErrors(err, e)
		}
	}

	for id, sink := range sinks {
		e := t.rollbackNode(pgfe, sink, sinkAdd[id], sinkDel[id])
		if e != nil {
			err = chainErrors(err, e)
		}
	}

	return err
}

func (t *testerImpl) rollbackNode(pgfe *nifi.ProcessGroupFlowEntity,
	node *nifi.ProcessorEntity,
	added, deleted []*nifi.ConnectionEntity) error {

	_, err := t.deleteConns(added)
	if err != nil {
		return err
	}
	_, err = t.addConns(pgfe, deleted)
	if err != nil {
		return err
	}
	_, err = t.deleteProc(node)
	if err != nil {
		return err
	}
	return nil
}

func (t *testerImpl) deleteProc(p *nifi.ProcessorEntity) (
	*nifi.ProcessorEntity, error) {

	// we have to fetch it again because we may have changed its revision
	// if we moved it on the canvas
	node, h, body, err := t.client.ProcessorsApi.GetProcessor(t.ctx, p.Id)
	err = t.handleErr(err, h, body, 200, "GetProcessor")
	if err != nil {
		return nil, err
	}

	version := optional.NewString(fmt.Sprintf("%v", *node.Revision.Version))
	proc, h, body, err := t.client.ProcessorsApi.DeleteProcessor(
		t.ctx,
		node.Id,
		&nifi.ProcessorsApiDeleteProcessorOpts{
			Version: version,
		})
	err = t.handleErr(err, h, body, 200, "DeleteProcessor")
	if err != nil {
		return nil, err
	}
	t.log.Debug("Deleted " + proc.Id)

	return &proc, nil
}

func (t *testerImpl) addConns(pgfe *nifi.ProcessGroupFlowEntity,
	conns []*nifi.ConnectionEntity) ([]*nifi.ConnectionEntity, error) {

	added := []*nifi.ConnectionEntity{}
	for _, conn := range conns {
		c, err := t.addConn(pgfe, conn)
		if err != nil {
			return nil, err
		}
		added = append(added, c)
	}
	return added, nil
}

func (t *testerImpl) addConn(pgfe *nifi.ProcessGroupFlowEntity,
	conn *nifi.ConnectionEntity) (*nifi.ConnectionEntity, error) {

	var version int64 = 0
	toAdd := *conn
	toAdd.Revision = &nifi.RevisionDto{Version: &version}
	toAdd.Id = ""
	component := *conn.Component
	component.Id = ""
	toAdd.Component = &component
	c, h, body, err := t.client.ProcessGroupsApi.CreateConnection(
		t.ctx,
		pgfe.ProcessGroupFlow.Id,
		toAdd,
	)
	err = t.handleErr(err, h, body, 201, "CreateConnection")
	if err != nil {
		return nil, err
	}
	t.log.Debug("Connected " + c.SourceId + " -> " + c.DestinationId)

	return &c, nil
}

func (t *testerImpl) deleteConns(conns []*nifi.ConnectionEntity) (
	[]*nifi.ConnectionEntity, error) {

	deletedConns := []*nifi.ConnectionEntity{}
	for _, conn := range conns {
		c, err := t.deleteConn(conn)
		if err != nil {
			return deletedConns, err
		}
		deletedConns = append(deletedConns, c)
	}
	return deletedConns, nil
}

func (t *testerImpl) deleteConn(c *nifi.ConnectionEntity) (
	*nifi.ConnectionEntity, error) {

	version := optional.NewString(fmt.Sprintf("%v", *c.Revision.Version))
	conn, h, body, err := t.client.ConnectionsApi.DeleteConnection(
		t.ctx,
		c.Id,
		&nifi.ConnectionsApiDeleteConnectionOpts{
			Version: version,
		})
	err = t.handleErr(err, h, body, 200, "DeleteConnection")
	if err != nil {
		return nil, err
	}
	t.log.Debug("Disconnected " + c.SourceId + " -> " + c.DestinationId)

	return &conn, nil
}

func (t *testerImpl) handleErr(err error, h *http.Response, body *string,
	expected int, tag string) error {

	if err != nil {
		if body != nil {
			return fmt.Errorf("%v: %w: %v", tag, err, *body)
		}
		return err
	}
	if h.StatusCode != expected {
		if body != nil {
			return errors.New(tag + ": http status " + h.Status +
				". " + *body)
		}
		return errors.New(tag + ": http status " + h.Status)
	}
	return nil
}


func connsOutOfId(id Id, c []nifi.ConnectionEntity) []*nifi.ConnectionEntity {
	conns := []*nifi.ConnectionEntity{}
	for i, _ := range c {
		if c[i].SourceId == id.Id() && c[i].SourceGroupId == id.GroupId() {
			conns = append(conns, &c[i])
		}
	}
	return conns
}

func connsIntoId(id Id,	c []nifi.ConnectionEntity) []*nifi.ConnectionEntity {
	conns := []*nifi.ConnectionEntity{}
	for i, _ := range c {
		if c[i].DestinationId == id.Id() && c[i].DestinationGroupId == id.GroupId() {
			conns = append(conns, &c[i])
		}
	}
	return conns
}

func getClient(urlString string, proxyURL string, verifySrvCert bool) (
	*nifi.APIClient, error) {

	url, err := url.ParseRequestURI(urlString)
	if err != nil {
		return nil, err
	}
	client, err := getHTTPClient(url.Scheme, proxyURL, verifySrvCert)
	if err != nil {
		return nil, err
	}
	cfg := &nifi.Configuration{
		BasePath:      urlString,
		Host:          url.Host,
		Scheme:        url.Scheme,
		DefaultHeader: make(map[string]string),
		UserAgent:     "NifiKop",
		HTTPClient:    client,
	}
	return nifi.NewAPIClient(cfg), nil
}

func getHTTPClient(scheme string, proxyURL string, verifySrvCert bool) (
	*http.Client, error) {

	transport, err := getTransport(scheme, proxyURL, verifySrvCert)
	if err != nil {
		return nil, err
	}
	return &http.Client{Transport: transport}, nil
}

func getTransport(scheme string, proxyURL string, verifySrvCert bool) (
	*http.Transport, error) {

	pxy, err := getProxy(proxyURL)
	if err != nil {
		return nil, err
	}
	tls := getTLS(scheme, verifySrvCert)
	if pxy != nil || tls != nil {
		return &http.Transport{
			Proxy:           pxy,
			TLSClientConfig: tls,
		}, nil
	}
	return nil, nil
}

func getProxy(proxyURL string) (func (*http.Request) (*url.URL, error), error) {
	if proxyURL != "" {
		url, err := url.ParseRequestURI(proxyURL)
		if err != nil {
			return nil, err
		}
		return http.ProxyURL(url), nil
	}
	return nil, nil
}

func getTLS(scheme string, verifySrvCert bool) *tls.Config {
	if scheme == "https" {
		return &tls.Config{
			Rand:               rand.Reader,
			InsecureSkipVerify: !verifySrvCert,
		}
	}
	return nil
}

func getToken(ctx context.Context, client *nifi.APIClient, cfg *Config,
	log *zap.Logger) (string, error) {

	if cfg.Username.IsSet() {
		_, h, token, err := client.AccessApi.CreateAccessToken(
			ctx,
			&nifi.AccessApiCreateAccessTokenOpts{
				Username: cfg.Username,
				Password: cfg.Password,
			})
		if err != nil {
			return "", err
		}
		if h.StatusCode != 201 {
			if token != nil {
				return "", errors.New("Creating token: http status " +
					h.Status + ". " + *token)
			}
			return "", errors.New("Creating token: http status " +
				h.Status)
		}
		if token == nil {
			return "", errors.New("Nil token")
		}
		// log.Debug("token = " + *token)
		return *token, nil
	}
	return "", nil
}

func logConns(log *zap.Logger, conns []nifi.ConnectionEntity, tag string) {
	for i, _ := range conns {
		log.Debug(tag + ": " + conns[i].SourceId + " -> " +
			conns[i].DestinationId)
	}
}

func logConnsPtr(log *zap.Logger, conns []*nifi.ConnectionEntity, tag string) {
	for i, _ := range conns {
		log.Debug(tag + ": " + conns[i].SourceId + " -> " +
			conns[i].DestinationId)
	}
}

func cronSpec() string {
	// min	hour	day	month	dow	command
	now := time.Now()
	unix := now.Unix()
	roundUp := ((unix + 59) / 60) * 60
	secs := roundUp - unix
	if secs < 10 {
		unix = roundUp + 60
	} else {
		unix = roundUp
	}
	t := time.Unix(unix, 0)
	return fmt.Sprintf("%v\t%v\t%v\t%v\t*\t?", t.Minute(), t.Hour(),
		t.Day(), int(t.Month()))
}
