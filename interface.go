package mqrpc

import "time"

// type MessageService interface {
// Identifier() string
// Send(to string, msgType MsgType, payload interface{}) error
// SendToAny(msgType MsgType, payload interface{}) error
// Broadcast(msgType MsgType, payload interface{}) error
// Request(to string, msgType MsgType, payload interface{}, timeout time.Duration) (*Message, error)
// }

type DefaultMessageService struct {
	MqService *MqService
}

func (m *DefaultMessageService) Identifier() string {
	return m.MqService.peerName
}

func (m *DefaultMessageService) Send(to string, msgType MsgType, payload interface{}) error {
	return m.MqService.fireAndForget(to, msgType, payload, false)
}

func (m *DefaultMessageService) SendToAny(msgType MsgType, payload interface{}) error {
	return m.MqService.fireAndForget("", msgType, payload, false)
}

func (m *DefaultMessageService) Broadcast(msgType MsgType, payload interface{}) error {
	return m.MqService.fireAndForget("", msgType, payload, true)
}

func (m *DefaultMessageService) Request(to string, msgType MsgType, payload interface{}, timeout time.Duration) (*Message, error) {
	return m.MqService.sendAndWaitReply(to, msgType, payload, timeout)
}
