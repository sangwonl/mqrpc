package mqrpc

type Route struct {
	MsgType string
	Handler HandlerFunc
}
