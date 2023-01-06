package nifitest

import (
	"github.com/konpyutaika/nigoapi/pkg/nifi"
	"github.com/antihax/optional"
	"go.uber.org/zap"

	"bufio"
	"os"

	"time"
	"fmt"
	"strings"
	"context"
	"bytes"
	"errors"
	"io"
	"encoding/binary"
	"net/url"
	"net/http"
	"crypto/tls"
	"crypto/rand"
	"hash/crc32"
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
	// token, err := getToken(ctx, clnt, cfg, log)
	// if err != nil {
	// 	return nil, err
	// }
	// ctx = context.WithValue(ctx, nifi.ContextAccessToken, token)
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
	ports := make(map[Id]*nifi.PortEntity)
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
				ports, sinkAdd, sinkDel, err)
		}
	}

	for id, _ := range expected {
		incoming := connsIntoId(id, conns.Connections)
		// creates a sink
		// deletes all incoming connections into id
		// creates connections from the sources of the former incoming,
		// connections to the sink
		port, addConns, delConns, err :=
			t.createSink(&pgfe, incoming, id)
		if port != nil {
			ports[id] = port
		}
		if addConns != nil {
			sinkAdd[id] = addConns
		}
		if delConns != nil {
			sinkDel[id] = delConns
		}
		if err != nil {
			return t.rollback(&pgfe, injectors, srcAdd, srcDel,
				ports, sinkAdd, sinkDel, err)
		}
	}

	reader := bufio.NewReader(os.Stdin)
	reader.ReadString('\n')

	excluded := make(map[string]interface{})
	excludeNodes(excluded, injectors)
	excludePorts(excluded, ports)
	/*started*/_, err = t.startFlow(&pgfe, injectors, ports, excluded)
	if err != nil {
		return t.rollback(&pgfe, injectors, srcAdd, srcDel, ports,
			sinkAdd, sinkDel, err)
	}

	// reader.ReadString('\n')

	results, err := t.getResults(ports)
	if err != nil {
		return t.rollback(&pgfe, injectors, srcAdd, srcDel, ports,
			sinkAdd, sinkDel, err)
	}
	for id, r := range expected {
		actual, found := results[id]
		if !found || actual != r {
			t.log.Error("Output " + id.Id() + ": expected: " + r +
				", got: " + actual)
		}
	}

	return t.rollback(&pgfe, injectors, srcAdd, srcDel, ports, sinkAdd,
		sinkDel, err)
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

	t.log.Debug("Created processor " + injector.Id)
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
	port *nifi.PortEntity, connected, disconnected []*nifi.ConnectionEntity,
	err error) {

	port, err = t.doCreatePort(pgfe, id.Id())
	if err != nil {
		return nil, nil, nil, err
	}

	disconnected, err = t.deleteConns(in)
	if err != nil {
		return port, nil, disconnected, err
	}

	connected, err = t.connectPort(pgfe, disconnected, port)
	if err != nil {
		return port, connected, disconnected, err
	}
	return port, connected, disconnected, nil
}

func (t *testerImpl) doCreatePort(pgfe *nifi.ProcessGroupFlowEntity,
	id string) (*nifi.PortEntity, error) {

	var version int64 = 0
	port, h, body, err := t.client.ProcessGroupsApi.CreateOutputPort(
		t.ctx,
		pgfe.ProcessGroupFlow.Id,
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

func (t *testerImpl) connectPort(pgfe *nifi.ProcessGroupFlowEntity,
	disconn []*nifi.ConnectionEntity, port *nifi.PortEntity) (
	[]*nifi.ConnectionEntity, error) {

	conns := []*nifi.ConnectionEntity{}
	for _, conn := range disconn {
		c, err := t.doConnectPort(pgfe, conn, port)
		if err != nil {
			return conns, err
		}
		conns = append(conns, c)
	}
	return conns, nil
}

func (t *testerImpl) doConnectPort(pgfe *nifi.ProcessGroupFlowEntity,
	src *nifi.ConnectionEntity, dst *nifi.PortEntity) (
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

func (t *testerImpl) startFlow(pgfe *nifi.ProcessGroupFlowEntity,
	injectors map[Id]*nifi.ProcessorEntity, ports map[Id]*nifi.PortEntity,
	excluded map[string]interface{}) ([]string, error) {

	started := []string{}
	for _, inj := range injectors {
		err := t.startProc(inj)
		if err != nil {
			return started, err
		}
		started = append(started, inj.Id)
	}

	for i, _ := range pgfe.ProcessGroupFlow.Flow.Processors {
		proc := &pgfe.ProcessGroupFlow.Flow.Processors[i]
		if _, found := excluded[proc.Id]; !found {
			err := t.startProc(proc)
			if err != nil {
				return started, err
			}
			started = append(started, proc.Id)
		}
	}

	for _, port := range ports {
		err := t.startPort(port)
		if err != nil {
			return started, err
		}
		started = append(started, port.Id)
	}

	return started, nil
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

func (t *testerImpl) getResults(ports map[Id]*nifi.PortEntity) (
	map[Id]string, error) {

	r := make(map[Id]string)
	for id, port := range ports {
		d, err := t.fetchData(port)
		if err != nil {
			return r, err
		}
		r[id] = d
	}
	return r, nil
}

func (t *testerImpl) fetchData(port *nifi.PortEntity) (string, error) {
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
	tx := tid.Message[colon+1:]

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

	checksum := crc32.ChecksumIEEE([]byte(*packet))
	const CONFIRM_TRANSACTION = int32(12)
	_, h, body, err = t.client.DataTransferApi.CommitOutputPortTransaction(
		t.ctx,
		CONFIRM_TRANSACTION,
		fmt.Sprintf("%v", checksum),
		port.Id,
		tx)
	err = t.handleErr(err, h, body, 200, "CommitOutputPortTransaction")
	if err != nil {
		return "", err
	}

	s, err := deserializePacket([]byte(*packet))
	if err != nil {
		return "", err
	}
	t.log.Debug("Port " + port.Id + ": " + s)
	return s, err
}

/*
   00000003
   00000004 70617468
   00000002 2e2f
   
   00000008 66696c656e616d65
   00000024 33663538396637372d323534342d343734362d623637382d373562396339663335333463
   
   00000004 75756964
   00000024 33663538396637372d323534342d343734362d623637382d373562396339663335333463

   0000000000000003 313233
 */
func deserializePacket(packet []byte) (string, error) {
	r := bytes.NewReader(packet)
	if r == nil {
		return "", errors.New("Could not get a byte reader")
	}

	var N int32
	err := binary.Read(r, binary.BigEndian, &N)
	if err != nil {
		return "", chainErrors(
			errors.New("Could not read the number of attributes"),
			err)
	}
	for i := 0; i < int(N); i++ {
		_, _, err := readKV(r)
		if err != nil {
			return "", err
		}
	}

	var S int64
	err = binary.Read(r, binary.BigEndian, &S)
	if err != nil {
		return "", chainErrors(
			errors.New("Could not read the data size"), err)
	}

	d := []byte{}
	var read int64 = 0
	for {
		buf := make([]byte, S - read)
		size, err := r.Read(buf)
		read += int64(size)
		if err != nil {
			if err != io.EOF {
				return "", chainErrors(
					errors.New("Reading data"), err)
			}
			if read < S {
				return "", errors.New("Premature EOF")
			}
			d = append(d, buf[0:size]...)
			break
		}
		d = append(d, buf[0:size]...)
		if read == S {
			size, err = r.Read(buf)
			if err == nil || err != io.EOF || size > 0 {
				return "", errors.New("Trailing garbage")
			}
			break
		}
	}

	return string(d), nil
}

func readKV(r io.Reader) (string, string, error) {
	k, err := readString(r)
	if err != nil {
		return "", "", err
	}
	v, err := readString(r)
	if err != nil {
		return k, "", err
	}
	return k, v, nil
}

func readString(r io.Reader) (string, error) {
	var S int32
	err := binary.Read(r, binary.BigEndian, &S)
	if err != nil {
		return "", chainErrors(
			errors.New("Could not read the string length"),	err)
	}
	b := make([]byte, S)
	n, err := r.Read(b)
	if n < int(S) {
		return string(b), errors.New("Incomplete string")
	}
	if err != nil && err != io.EOF {
		return string(b), err
	}
	return string(b), nil
}

func (t *testerImpl) rollback(pgfe *nifi.ProcessGroupFlowEntity,
	injectors map[Id]*nifi.ProcessorEntity,
	injAdd, injDel map[Id][]*nifi.ConnectionEntity,
	ports map[Id]*nifi.PortEntity,
	sinkAdd, sinkDel map[Id][]*nifi.ConnectionEntity, err error) error {

	for id, port := range ports {
		e := t.rollbackPort(pgfe, port, sinkAdd[id], sinkDel[id])
		if e != nil {
			return chainErrors(err, e)
		}
	}

	for id, injector := range injectors {
		e := t.rollbackNode(pgfe, injector, injAdd[id], injDel[id])
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
	t.log.Debug("Deleted processor " + proc.Id)

	return &proc, nil
}

func (t *testerImpl) rollbackPort(pgfe *nifi.ProcessGroupFlowEntity,
	p *nifi.PortEntity, added, deleted []*nifi.ConnectionEntity) error {

	port, err := t.stopPort(p)
	if err != nil {
		return err
	}

	_, err = t.deleteConns(added)
	if err != nil {
		return err
	}
	_, err = t.addConns(pgfe, deleted)
	if err != nil {
		return err
	}
	_, err = t.deletePort(port)
	if err != nil {
		return err
	}
	return nil
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
	return &http.Transport{
		Proxy:           pxy,
		TLSClientConfig: tls,
	}, nil
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

func excludeNodes(ids map[string]interface{},
	toExclude map[Id]*nifi.ProcessorEntity) {

	for id, _ := range toExclude {
		ids[id.Id()] = nil
	}
}

func excludePorts(ids map[string]interface{},
	toExclude map[Id]*nifi.PortEntity) {

	for id, _ := range toExclude {
		ids[id.Id()] = nil
	}
}

