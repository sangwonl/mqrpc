package mqrpc

import "time"

// type MessageService interface {
// Identifier() string
// Send(to, msgType string, payload interface{}) error
// SendToAny(msgType string, payload interface{}) error
// Broadcast(msgType string, payload interface{}) error
// Request(to, msgType string, payload interface{}, timeout time.Duration) (interface{}, error)
// }

type DefaultMessageService struct {
	MqService *MqService
}

func (m *DefaultMessageService) Identifier() string {
	return m.MqService.peerName
}

func (m *DefaultMessageService) Send(to, msgType string, payload interface{}) error {
	return m.MqService.fireAndForget(to, MsgType(msgType), payload, false)
}

func (m *DefaultMessageService) SendToAny(msgType string, payload interface{}) error {
	return m.MqService.fireAndForget("", MsgType(msgType), payload, false)
}

func (m *DefaultMessageService) Broadcast(msgType string, payload interface{}) error {
	return m.MqService.fireAndForget("", MsgType(msgType), payload, true)
}

func (m *DefaultMessageService) Request(to, msgType string, payload interface{}, timeout time.Duration) (interface{}, error) {
	msg, err := m.MqService.sendAndWaitReply(to, MsgType(msgType), payload, timeout)
	if err != nil {
		return nil, err
	}
	return msg.Payload, nil
}
