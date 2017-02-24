package proto

import (
	"net"
	"net/rpc"
	"time"

	"github.com/ugorji/go/codec"
)

func DialMsgpack(addr string, timeout time.Duration) (*rpc.Client, error) {
	conn, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	_, err = conn.Write([]byte{byte(RpcProto)})
	if err != nil {
		conn.Close()
		return nil, err
	}
	codec := codec.MsgpackSpecRpc.ClientCodec(conn, &MsgpackHandle)
	return rpc.NewClientWithCodec(codec), nil
}
