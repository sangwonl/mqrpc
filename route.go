package mq

type Route struct {
	MsgType string
	Handler HandlerFunc
}
