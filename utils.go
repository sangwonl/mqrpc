package mqrpc

import (
	"encoding/json"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

func composeMessage(msgType MsgType, msgId, replyTo string, payload interface{}) amqp.Publishing {
	if msgId == "" {
		msgId = uuid.NewV4().String()
	} else { // assume it's for reply
		msgType = MsgTypeReservedReply
	}

	body, _ := json.Marshal(payload)

	msg := amqp.Publishing{
		MessageId:   msgId,
		ContentType: "application/json",
		Type:        string(msgType),
		Body:        body,
	}

	if replyTo != "" {
		msg.ReplyTo = replyTo
	}

	return msg
}

func receiveMessageWithTimeout(ch chan Message, timeout time.Duration) Message {
	const DefaultTimeout = 15 * time.Second
	if timeout == 0 {
		timeout = DefaultTimeout
	}

	var recvMsg Message
	select {
	case recvMsg = <-ch:
	case <-time.After(timeout):
	}
	return recvMsg
}
