package service

import (
	"fmt"
	"io"
	"net"

	"github.com/skyshore/asynctask/proto"
	"github.com/hashicorp/memberlist"
	"github.com/ugorji/go/codec"
)

// listen is used to listen for incoming RPC connections
func (s *Server) listen() {
	for {
		// Accept a connection
		conn, err := s.rpcListener.Accept()
		if err != nil {
			/*if s.shutdown {
				return
			}*/
			s.logger.Error(fmt.Sprintf("[ERR] consul.rpc: failed to accept RPC conn: %v", err))
			continue
		}

		go s.handleConn(conn)
	}
}

// logConn is a wrapper around memberlist's LogConn so that we format references
// to "from" addresses in a consistent way. This is just a shorter name.
func logConn(conn net.Conn) string {
	return memberlist.LogConn(conn)
}
func (s *Server) handleConn(conn net.Conn) {
	// Read a single byte
	buf := make([]byte, 1)
	if _, err := conn.Read(buf); err != nil {
		if err != io.EOF {
			s.logger.Error(fmt.Sprintf("[ERR] consul.rpc: failed to read byte: %v %s", err, logConn(conn)))
		}
		conn.Close()
		return
	}

	// Switch on the byte
	switch proto.RPCType(buf[0]) {
	case proto.RpcProto:
		s.handleAsynctaskConn(conn)
	case proto.RaftProto:
		s.raftLayer.Handoff(conn)
	default:
		s.logger.Error(fmt.Sprintf("[ERR] consul.rpc: unrecognized RPC byte: %v %s", buf[0], logConn(conn)))
		conn.Close()
		return

	}
}

// handleConsulConn is used to service a single asynctask RPC connection
func (s *Server) handleAsynctaskConn(conn net.Conn) {
	s.logger.Debug("handle rpc...")
	codec := codec.MsgpackSpecRpc.ServerCodec(conn, &proto.MsgpackHandle)
	s.rpcServer.ServeCodec(codec)
}
