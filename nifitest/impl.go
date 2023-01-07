package nifitest

import (
	"github.com/konpyutaika/nigoapi/pkg/nifi"
	"github.com/antihax/optional"
	"go.uber.org/zap"

	// "bufio"
	// "os"

	"fmt"
	"strings"
	"context"
	"errors"
	"net/url"
	"net/http"
	"hash/crc32"
	"sync/atomic"
)

type testerImpl struct {
	log            *zap.Logger
	ctx            context.Context
	client         *nifi.APIClient
	removeOutLinks bool

	state          State
	busy           *atomic.Bool

	pgfe           *nifi.ProcessGroupFlowEntity
	injectors      map[string]*nifi.ProcessorEntity
	ports          map[string]*nifi.PortEntity
	sinkAdd        map[string][]*nifi.ConnectionEntity
	sinkDel        map[string][]*nifi.ConnectionEntity
	started        map[string]interface{}
	tx             map[string]string
	result         chan bool
	error          chan error
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
	// token, err := getToken(ctx, clnt, cfg, log)
	// if err != nil {
	// 	return nil, err
	// }
	// ctx = context.WithValue(ctx, nifi.ContextAccessToken, token)
	var flag atomic.Bool
	flag.Store(false)
	return &testerImpl{
		log:            log,
		ctx:            ctx,
		client:         clnt,
		removeOutLinks: cfg.RemoveOutLinks,
		state:          Idle,
		busy:           &flag,
		injectors:      make(map[string]*nifi.ProcessorEntity),
		ports:          make(map[string]*nifi.PortEntity),
		sinkAdd:        make(map[string][]*nifi.ConnectionEntity),
		sinkDel:        make(map[string][]*nifi.ConnectionEntity),
		started:        make(map[string]interface{}),
		tx:             make(map[string]string),
		result:         make(chan bool, 1),
		error:          make(chan error, 1),
	}, nil
}

func (t *testerImpl) State() State {
	return t.state
}

func (t *testerImpl) TestAsync(testData, expected map[string]string) (
	optional.Bool, error) {

	if !t.busy.CompareAndSwap(false, true) {
		return optional.EmptyBool(), errors.New("Busy")
	}
	defer t.busy.Store(false)

	if t.state != Idle {
		return optional.EmptyBool(),
			errors.New("Another test in progress")
	}

	err := t.doStart(testData, keys(expected))
	if err != nil {
		t.state = Error
		return optional.EmptyBool(), err
	}
	t.state = Initialized

	err = t.doExecute()
	if err != nil {
		t.state = Error
		return optional.EmptyBool(), err
	}

	return t.launch(expected)
}

func (t *testerImpl) doStart(testData map[string]string, sinks []string) error {
	pgfe, h, body, err := t.client.FlowApi.GetFlow(
		t.ctx,
		"root",
		&nifi.FlowApiGetFlowOpts{
			UiOnly: optional.NewBool(false),
		})
	err = t.handleErr(err, h, body, 200, "GetFlow")
	if err != nil {
		return err
	}
	t.pgfe = &pgfe

	conns, h, body, err := t.client.ProcessGroupsApi.GetConnections(
		t.ctx, pgfe.ProcessGroupFlow.Id)
	err = t.handleErr(err, h, body, 200, "GetConnections")
	if err != nil {
		return err
	}

	for id, toInject := range testData {
		outgoing := connsOutOfId(id, conns.Connections)
		// creates an injector
		// creates connections from the injector to the destinations of
		// the former outgoing connections of id
		injector, err := t.createInjector(outgoing, id, toInject)
		if injector != nil {
			t.injectors[id] = injector
		}
		if err != nil {
			return err
		}
	}

	for _, id := range sinks {
		incoming := connsIntoId(id, conns.Connections)
		// creates a sink
		// deletes all incoming connections into id
		// creates connections from the sources of the former incoming,
		// connections to the sink
		port, addConns, delConns, err := t.createPort(incoming, id)
		if port != nil {
			t.ports[id] = port
		}
		if addConns != nil {
			t.sinkAdd[id] = addConns
		}
		if delConns != nil {
			t.sinkDel[id] = delConns
		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *testerImpl) doExecute() error {
	excluded := make(map[string]interface{})
	excludeNodes(excluded, t.injectors)
	if t.removeOutLinks {
		excludePorts(excluded, t.ports)
	}
	return t.startFlow(excluded)
}

func (t *testerImpl) launch(expected map[string]string) (optional.Bool, error) {
	go t.check(expected)

	done, err := t.poll()
	if !done.IsSet() {
		t.state = Executing
	}
	return done, err
}

func (t *testerImpl) Check() (optional.Bool, error) {
	if !t.busy.CompareAndSwap(false, true) {
		return optional.EmptyBool(), errors.New("Busy")
	}
	defer t.busy.Store(false)

	switch t.state {
	case Executing:
	case Error:
	default:
		return optional.EmptyBool(),
			errors.New("Flow execution has not been started")
	}

	return t.poll()
}

func (t *testerImpl) poll() (optional.Bool, error) {
	select {
	case err := <- t.error:
		if err == nil {
			err = t.doRollback(nil)
			if err != nil {
				t.state = Error
			} else {
				t.state = Idle
			}
			return optional.NewBool(<- t.result), err
		}
		t.state = Error
		return optional.NewBool(false), err
	default:
		return optional.EmptyBool(), nil
	}
}

func (t *testerImpl) check(expected map[string]string) {
	allOk, err := t.doCheck(expected)
	if err == nil {
		t.result <- allOk.Value()
	}
	t.error <- err
}

func (t *testerImpl) doCheck(exp map[string]string) (optional.Bool, error) {
	results, err := t.getResults()
	if err != nil {
		return optional.EmptyBool(), err
	}

	result := t.compare(results, exp)
	if result {
		t.log.Info("The test has passed")
	} else {
		t.log.Info("The test has failed")
	}
	return optional.NewBool(result), nil
}

func (t *testerImpl) getResults() (map[string]string, error) {
	r := make(map[string]string)
	for id, port := range t.ports {
		d, err := t.fetchData(port)
		if err != nil {
			return r, err
		}
		r[id] = d
	}
	return r, nil
}

func (t *testerImpl) fetchData(port *nifi.PortEntity) (string, error) {
	tid, err := t.createTransaction(port)
	if err != nil {
		return "", err
	}
	t.tx[port.Id] = tid

	packet, err := t.getPacket(port, tid)
	if err != nil {
		return "", err
	}

	checksum := crc32.ChecksumIEEE([]byte(packet))

	err = t.commitTransaction(port, tid, checksum)
	if err != nil {
		return "", err
	}
	delete(t.tx, port.Id)

	s, err := deserializePacket([]byte(packet))
	if err != nil {
		return "", err
	}
	t.log.Debug("Port " + port.Id + ": " + s)
	return s, err
}

func (t *testerImpl) createTransaction(port *nifi.PortEntity) (string, error) {
	tid, h, body, err := t.client.DataTransferApi.CreatePortTransaction(
		t.ctx,
		"output-ports",
		port.Id)
	err = t.handleErr(err, h, body, 201, "CreatePortTransaction")
	if err != nil {
		return "", err
	}

	colon := strings.IndexRune(tid.Message, ':')
	if colon == -1 {
		return "", errors.New("Invalid message from CreatePortTransaction")
	}
	return tid.Message[colon+1:], nil
}

func (t *testerImpl) getPacket(port *nifi.PortEntity, tx string) (
	string, error) {

	_, h, packet, err := t.client.DataTransferApi.TransferFlowFiles(
		t.ctx,
		port.Id,
		tx)
	err = t.handleErr(err, h, packet, 202, "TransferFlowFiles")
	if err != nil {
		return "", err
	}

	if packet == nil {
		return "", errors.New("Null data when reading port")
	}

	return *packet, nil
}

func (t *testerImpl) commitTransaction(port *nifi.PortEntity, tx string,
	checksum uint32) error {

	const CONFIRM_TRANSACTION = int32(12)
	_, h, body, err := t.client.DataTransferApi.CommitOutputPortTransaction(
		t.ctx,
		CONFIRM_TRANSACTION,
		fmt.Sprintf("%v", checksum),
		port.Id,
		tx)
	return t.handleErr(err, h, body, 200, "CommitOutputPortTransaction")
}

func (t *testerImpl) compare(actual, expected map[string]string) bool {
	allOk := true
	for id, expectedValue := range expected {
		actualValue, found := actual[id]
		if !found || actualValue != expectedValue {
			allOk = false
			t.log.Error("Output " + id + ": expected: " +
				expectedValue + ", got: " + actualValue)
		}
	}
	return allOk
}

func (t *testerImpl) TestSync(testData, expected map[string]string) (
	bool, error) {

	if !t.busy.CompareAndSwap(false, true) {
		return false, errors.New("Busy")
	}
	defer t.busy.Store(false)

	err := t.doStart(testData, keys(expected))

	// reader := bufio.NewReader(os.Stdin)
	// reader.ReadString('\n')

	err = t.doExecute()
	if err != nil {
		return false, t.doRollback(err)
	}

	// reader.ReadString('\n')

	ok, err := t.doCheck(expected)
	result := ok.IsSet() && ok.Value()
	err = t.doRollback(err)
	return result, err
}

func (t *testerImpl) Rollback() error {
	if !t.busy.CompareAndSwap(false, true) {
		return errors.New("Busy")
	}
	defer t.busy.Store(false)

	err := t.doRollback(nil)
	if err == nil {
		t.state = Idle
	} else {
		t.state = Error
	}
	return err
}

func (t *testerImpl) doRollback(orig error) error {
	err1 := t.deleteTx()
	err2 := t.rollbackInjectors()
	err3 := t.rollbackPorts()
	if err1 == nil && err2 == nil && err3 == nil {
		t.pgfe = nil
		return nil
	}
	return chainErrors(orig, err1, err2, err3)
}

func (t *testerImpl) deleteTx() error {
	var err error = nil
	for id, tid := range t.tx {
		e := t.doDeleteTx(id, tid)
		if e == nil {
			delete(t.tx, id)
		} else {
			err = chainErrors(err, e)
		}
	}
	return err
}

func (t *testerImpl) doDeleteTx(id, tx string) error {
	const CANCEL_TRANSACTION = int32(15)
	_, h, body, err := t.client.DataTransferApi.CommitOutputPortTransaction(
		t.ctx,
		CANCEL_TRANSACTION,
		"",
		id,
		tx)
	return t.handleErr(err, h, body, 200, "CommitOutputPortTransaction")
}

func (t *testerImpl) rollbackInjectors() error {
	var err error = nil
	for id, injector := range t.injectors {
		_, e := t.deleteProc(injector)
		if e == nil {
			delete(t.injectors, id)
		} else {
			err = chainErrors(err, e)
		}
	}

	return err
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
	t.log.Debug("Deleted processor " + proc.Id)

	return &proc, nil
}

func (t *testerImpl) rollbackPorts() error {
	var err error = nil
	for id, port := range t.ports {
		e := t.rollbackPort(id, port)
		if e != nil {
			err = chainErrors(err, e)
		}
	}

	return err
}

func (t *testerImpl) rollbackPort(id string, p *nifi.PortEntity) error {
	rc := t.reconnect(id)

	var port *nifi.PortEntity
	var err error
	if _, ok := t.started[p.Id]; ok {
		port, err = t.stopPort(p)
		if err != nil {
			return chainErrors(rc, err)
		}
		delete(t.started, p.Id)
	} else {
		port = p
	}

	err = t.deconnect(id)
	if err != nil {
		return chainErrors(rc, err)
	}

	_, err = t.deletePort(port)
	if err != nil {
		return chainErrors(rc, err)
	}
	delete(t.ports, id)

	return rc
}

func (t *testerImpl) reconnect(id string) error {
	_, failed, err := t.addConns(t.sinkDel[id])
	if len(failed) == 0 {
		delete(t.sinkDel, id)
	} else {
		t.sinkDel[id] = failed
	}
	return err
}

func (t *testerImpl) deconnect(id string) error {
	_, failed, err := t.deleteConns(t.sinkAdd[id])
	if len(failed) == 0 {
		delete(t.sinkAdd, id)
	} else {
		t.sinkAdd[id] = failed
	}
	return err
}

func (t *testerImpl) stopPort(p *nifi.PortEntity) (*nifi.PortEntity, error) {
	// we have to fetch it again because we may have changed its revision
	// if we moved it on the canvas
	port, h, body, err := t.client.OutputPortsApi.GetOutputPort(t.ctx, p.Id)
	err = t.handleErr(err, h, body, 200, "GetOutputPort")
	if err != nil {
		return nil, err
	}

	_, h, body, err = t.client.OutputPortsApi.UpdateRunStatus(
		t.ctx,
		port.Id,
		nifi.PortRunStatusEntity{
			Revision: port.Revision,
			DisconnectedNodeAcknowledged: false,
			State: "STOPPED",
		})
	err = t.handleErr(err, h, body, 200, "UpdateRunStatus")
	if err != nil {
		return nil, err
	}
	t.log.Debug("Stopped port " + port.Id)
	return &port, nil
}

func (t *testerImpl) deletePort(p *nifi.PortEntity) (*nifi.PortEntity, error) {
	// we have to fetch it again because we may have changed its revision
	// if we moved it on the canvas
	prt, h, body, err := t.client.OutputPortsApi.GetOutputPort(t.ctx, p.Id)
	err = t.handleErr(err, h, body, 200, "GetOutputPort")
	if err != nil {
		return nil, err
	}
	version := optional.NewString(fmt.Sprintf("%v", *prt.Revision.Version))
	port, h, body, err := t.client.OutputPortsApi.RemoveOutputPort(
		t.ctx,
		p.Id,
		&nifi.OutputPortsApiRemoveOutputPortOpts{
			Version: version,
		},
	)
	err = t.handleErr(err, h, body, 200, "RemoveOutputPort")
	if err != nil {
		return nil, err
	}
	t.log.Debug("Deleted port " + port.Id)
	return &port, err
}

func (t *testerImpl) createInjector(out []*nifi.ConnectionEntity,
	id, toInject string) (injector *nifi.ProcessorEntity, err error) {

	injector, err = t.doCreateInjector(id, toInject)
	if err != nil {
		return nil, err
	}

	_, err = t.connectInjector(injector, out)
	if err != nil {
		return injector, err
	}
	return injector, nil
}

func (t *testerImpl) doCreateInjector(id, toInject string) (
	*nifi.ProcessorEntity, error) {

	var version int64 = 0
	injector, h, body, err := t.client.ProcessGroupsApi.CreateProcessor(
		t.ctx,
		t.pgfe.ProcessGroupFlow.Id,
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

	t.log.Debug("Created processor " + injector.Id)
	return &injector, err
}

func (t *testerImpl) connectInjector(injector *nifi.ProcessorEntity,
	disconn []*nifi.ConnectionEntity) ([]*nifi.ConnectionEntity, error) {

	conns := []*nifi.ConnectionEntity{}
	for _, conn := range disconn {
		c, err := t.doConnectInjector(injector, conn)
		if err != nil {
			return conns, err
		}
		conns = append(conns, c)
	}
	return conns, nil
}

func (t *testerImpl) doConnectInjector(src *nifi.ProcessorEntity,
	dst *nifi.ConnectionEntity) (*nifi.ConnectionEntity, error) {

	var version int64 = 0
	conn, h, body, err := t.client.ProcessGroupsApi.CreateConnection(
		t.ctx,
		t.pgfe.ProcessGroupFlow.Id,
		nifi.ConnectionEntity{
			Revision: &nifi.RevisionDto{
				Version: &version,
			},
			DisconnectedNodeAcknowledged: false,
			Component: &nifi.ConnectionDto{
				Source: &nifi.ConnectableDto{
					GroupId: t.pgfe.ProcessGroupFlow.Id,
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

func (t *testerImpl) createPort(in []*nifi.ConnectionEntity, id string) (
	port *nifi.PortEntity,
	connected, disconnected []*nifi.ConnectionEntity, err error) {

	port, err = t.doCreatePort(id)
	if err != nil {
		return nil, nil, nil, err
	}

	if t.removeOutLinks {
		disconnected, _, err = t.deleteConns(in)
		if err != nil {
			return port, nil, disconnected, err
		}
		connected, err = t.connectPort(disconnected, port)
	} else {
		disconnected = []*nifi.ConnectionEntity{}
		connected, err = t.connectPort(in, port)
	}

	if err != nil {
		return port, connected, disconnected, err
	}
	return port, connected, disconnected, nil
}

func (t *testerImpl) doCreatePort(id string) (*nifi.PortEntity, error) {
	var version int64 = 0
	port, h, body, err := t.client.ProcessGroupsApi.CreateOutputPort(
		t.ctx,
		t.pgfe.ProcessGroupFlow.Id,
		nifi.PortEntity{
			Revision: &nifi.RevisionDto{
				Version: &version,
			},
			DisconnectedNodeAcknowledged: false,
			Component: &nifi.PortDto{
				Name:  "Port_" + id,
				Type_: "OUTPUT_PORT",
				State: "STOPPED",
			},
			PortType: "OUTPUT_PORT",
		})
	err = t.handleErr(err, h, body, 201, "CreateOutputPort")
	if err != nil {
		return nil, err
	}
	t.log.Debug("Created port " + port.Id)
	return &port, err
}

func (t *testerImpl) connectPort(disconn []*nifi.ConnectionEntity,
	port *nifi.PortEntity) ([]*nifi.ConnectionEntity, error) {

	conns := []*nifi.ConnectionEntity{}
	for _, conn := range disconn {
		c, err := t.doConnectPort(conn, port)
		if err != nil {
			return conns, err
		}
		conns = append(conns, c)
	}
	return conns, nil
}

func (t *testerImpl) doConnectPort(src *nifi.ConnectionEntity,
	dst *nifi.PortEntity) (*nifi.ConnectionEntity, error) {

	var version int64 = 0
	conn, h, body, err := t.client.ProcessGroupsApi.CreateConnection(
		t.ctx,
		t.pgfe.ProcessGroupFlow.Id,
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
					GroupId: t.pgfe.ProcessGroupFlow.Id,
					Id: dst.Id,
					Type_: "OUTPUT_PORT",
				},
				BackPressureDataSizeThreshold: "1 GB",
				BackPressureObjectThreshold: 10000,
				FlowFileExpiration: "0 sec",
				LoadBalanceCompression: "DO_NOT_COMPRESS",
				LoadBalancePartitionAttribute: "",
				LoadBalanceStrategy: "DO_NOT_LOAD_BALANCE",
				Name: "",
				Prioritizers: []string{},
				SelectedRelationships: src.Component.SelectedRelationships,
			},
		})
	err = t.handleErr(err, h, body, 201, "CreateConnection")
	if err != nil {
		return nil, err
	}
	t.log.Debug("Connected " + src.SourceId + " -> " + dst.Id)

	return &conn, nil
}

func (t *testerImpl) startFlow(excluded map[string]interface{}) error {
	for _, inj := range t.injectors {
		err := t.startProc(inj)
		if err != nil {
			return err
		}
	}

	for i, _ := range t.pgfe.ProcessGroupFlow.Flow.Processors {
		proc := &t.pgfe.ProcessGroupFlow.Flow.Processors[i]
		if _, found := excluded[proc.Id]; !found {
			err := t.startProc(proc)
			if err != nil {
				return err
			}
		}
	}

	for _, port := range t.ports {
		err := t.startPort(port)
		if err != nil {
			return err
		}
		t.started[port.Id] = nil
	}

	return nil
}

func (t *testerImpl) startProc(p *nifi.ProcessorEntity) error {
	// we have to fetch it again because we may have changed its revision
	// if we moved it on the canvas
	node, h, body, err := t.client.ProcessorsApi.GetProcessor(t.ctx, p.Id)
	err = t.handleErr(err, h, body, 200, "GetProcessor")
	if err != nil {
		return err
	}

	_, h, body, err = t.client.ProcessorsApi.UpdateRunStatus(
		t.ctx,
		p.Id,
		nifi.ProcessorRunStatusEntity{
			Revision: node.Revision,
			DisconnectedNodeAcknowledged: false,
			State: "RUN_ONCE",
		})
	err = t.handleErr(err, h, body, 200, "UpdateRunStatus")
	if err != nil {
		return err
	}
	t.log.Debug("Started processor " + p.Id)
	return nil
}

func (t *testerImpl) startPort(p *nifi.PortEntity) error {
	// we have to fetch it again because we may have changed its revision
	// if we moved it on the canvas
	port, h, body, err := t.client.OutputPortsApi.GetOutputPort(t.ctx, p.Id)
	err = t.handleErr(err, h, body, 200, "GetOutputPort")
	if err != nil {
		return err
	}

	_, h, body, err = t.client.OutputPortsApi.UpdateRunStatus(
		t.ctx,
		p.Id,
		nifi.PortRunStatusEntity{
			Revision: port.Revision,
			DisconnectedNodeAcknowledged: false,
			State: "RUNNING",
		})
	err = t.handleErr(err, h, body, 200, "UpdateRunStatus")
	if err != nil {
		return err
	}
	t.log.Debug("Started port " + port.Id)
	return nil
}

func (t *testerImpl) addConns(conns []*nifi.ConnectionEntity) (
	[]*nifi.ConnectionEntity, []*nifi.ConnectionEntity, error) {

	added := []*nifi.ConnectionEntity{}
	failed := []*nifi.ConnectionEntity{}
	var err error = nil
	for _, conn := range conns {
		c, e := t.addConn(conn)
		if e != nil {
			failed = append(failed, conn)
			err = chainErrors(err, e)
		} else {
			added = append(added, c)
		}
	}
	return added, failed, err
}

func (t *testerImpl) addConn(conn *nifi.ConnectionEntity) (
	*nifi.ConnectionEntity, error) {

	var version int64 = 0
	toAdd := *conn
	toAdd.Revision = &nifi.RevisionDto{Version: &version}
	toAdd.Id = ""
	component := *conn.Component
	component.Id = ""
	toAdd.Component = &component
	c, h, body, err := t.client.ProcessGroupsApi.CreateConnection(
		t.ctx,
		t.pgfe.ProcessGroupFlow.Id,
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
	[]*nifi.ConnectionEntity, []*nifi.ConnectionEntity, error) {

	deletedConns := []*nifi.ConnectionEntity{}
	failed := []*nifi.ConnectionEntity{}
	var err error = nil
	for _, conn := range conns {
		c, e := t.deleteConn(conn)
		if err != nil {
			failed = append(failed, conn)
			err = chainErrors(err, e)
		} else {
			deletedConns = append(deletedConns, c)
		}
	}
	return deletedConns, failed, err
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

func connsOutOfId(id string,
	c []nifi.ConnectionEntity) []*nifi.ConnectionEntity {

	conns := []*nifi.ConnectionEntity{}
	for i, _ := range c {
		if c[i].SourceId == id {
			conns = append(conns, &c[i])
		}
	}
	return conns
}

func connsIntoId(id string,
	c []nifi.ConnectionEntity) []*nifi.ConnectionEntity {

	conns := []*nifi.ConnectionEntity{}
	for i, _ := range c {
		if c[i].DestinationId == id {
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
	dfltHdrs := make(map[string]string)
	dfltHdrs["x-nifi-site-to-site-protocol-version"] = "3";
	cfg := &nifi.Configuration{
		BasePath:      urlString,
		Host:          url.Host,
		Scheme:        url.Scheme,
		DefaultHeader: dfltHdrs,
		UserAgent:     "NifiKop",
		HTTPClient:    client,
	}
	return nifi.NewAPIClient(cfg), nil
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
			var msg string
			if token != nil {
				msg = *token
			} else {
				msg = "null token"
			}
			return "", chainErrors(err, errors.New(msg))
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

func excludeNodes(ids map[string]interface{},
	toExclude map[string]*nifi.ProcessorEntity) {

	for id, _ := range toExclude {
		ids[id] = nil
	}
}

func excludePorts(ids map[string]interface{},
	toExclude map[string]*nifi.PortEntity) {

	for id, _ := range toExclude {
		ids[id] = nil
	}
}

func (s State) String() string {
	switch s {
	case Idle:
		return "Idle"
	case Initialized:
		return "Initialized"
	case Executing:
		return "Executing"
	case Error:
		return "Error"
	default:
		return "Unknown state"
	}
}

func keys(m map[string]string) []string {
	k := []string{}
	for key, _ := range m {
		k = append(k, key)
	}
	return k
}

// func logConns(log *zap.Logger, conns []nifi.ConnectionEntity, tag string) {
// 	for i, _ := range conns {
// 		log.Debug(tag + ": " + conns[i].SourceId + " -> " +
// 			conns[i].DestinationId)
// 	}
// }

// func logConnsPtr(log *zap.Logger, conns []*nifi.ConnectionEntity, tag string) {
// 	for i, _ := range conns {
// 		log.Debug(tag + ": " + conns[i].SourceId + " -> " +
// 			conns[i].DestinationId)
// 	}
// }

// func cronSpec() string {
// 	// min	hour	day	month	dow	command
// 	now := time.Now()
// 	unix := now.Unix()
// 	roundUp := ((unix + 59) / 60) * 60
// 	secs := roundUp - unix
// 	if secs < 10 {
// 		unix = roundUp + 60
// 	} else {
// 		unix = roundUp
// 	}
// 	t := time.Unix(unix, 0)
// 	return fmt.Sprintf("%v\t%v\t%v\t%v\t*\t?", t.Minute(), t.Hour(),
// 		t.Day(), int(t.Month()))
// }
