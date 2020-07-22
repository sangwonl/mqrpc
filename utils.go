package mq

import (
	"encoding/json"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
)

func composeMessage(msgId, msgType, replyTo string, payload interface{}) amqp.Publishing {
	if msgId == "" {
		msgId = uuid.NewV4().String()
	} else { // assume it's for reply
		msgType = MSG_TYPE_RESERVED_REPLY
	}

	body, _ := json.Marshal(payload)

	msg := amqp.Publishing{
		MessageId:   msgId,
		ContentType: "application/json",
		Type:        msgType,
		Body:        body,
	}

	if replyTo != "" {
		msg.ReplyTo = replyTo
	}

	return msg
}

func receiveMessageWithTimeout(ch chan Message, timeoutInSecs time.Duration) Message {
	timeout := make(chan bool, 1)
	go func() {
		time.Sleep(timeoutInSecs * time.Second)
		timeout <- true
	}()

	var recvMsg Message
	select {
	case recvMsg = <-ch:
	case <-timeout:
	}

	return recvMsg
}
