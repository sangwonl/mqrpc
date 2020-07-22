package mq

import (
	"fmt"
	"sync"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/segmentio/ksuid"
	"github.com/streadway/amqp"
)

const MSG_TYPE_RESERVED_REPLY = "reply"

type Message struct {
	ReplyTo string // application use - address to reply to (ex: RPC)
	// CorrelationId string    // application use - correlation identifier
	MessageId string    // application use - message identifier
	Timestamp time.Time // application use - message timestamp
	Type      string    // application use - message type name
	// UserId        string    // application use - creating user - should be authenticated user
	// AppId         string    // application use - creating application id

	Payload []byte `mapstructure:"Body"`
}

type MessageService interface {
	Identifier() string
	AllParticipants() []string

	Send(to string, msgType string, payload interface{})
	Broadcast(msgType string, payload interface{})
	Request(to string, msgType string, payload interface{}) Message
}

type DefaultMessageServiceImpl struct {
	MqService *MqService
}

func (m *DefaultMessageServiceImpl) Identifier() string {
	return m.MqService.peerName
}

func (m *DefaultMessageServiceImpl) AllParticipants() []string {
	return nil
}

func (m *DefaultMessageServiceImpl) Send(to, msgType string, payload interface{}) {
	m.MqService.fireAndForget(to, msgType, payload)
}

func (m *DefaultMessageServiceImpl) Broadcast(msgType string, payload interface{}) {
	m.MqService.fireAndForget("", msgType, payload)
}

func (m *DefaultMessageServiceImpl) Request(to, msgType string, payload interface{}) Message {
	return m.MqService.sendAndWaitReply(to, msgType, payload)
}

type Context struct {
	message Message
}

func (c *Context) GetMessage() Message {
	return c.message
}

type HandlerFunc func(ctx *Context) interface{}

type MessageHandler interface {
	Routes() []Route
}

type MqService struct {
	channel           *amqp.Channel
	peerName          string
	exchangeP2P       string
	exchangeBroadcast string
	queueP2P          *amqp.Queue
	queueBroadcast    *amqp.Queue

	recvMsgChannel     chan Message
	recvMsgChannelsRpc map[string]chan Message

	handlers     []MessageHandler
	handlerFuncs map[string]HandlerFunc
}

func (mq *MqService) Run(peerName string) {
	// generate unique peer name
	if peerName == "" {
		peerName = ksuid.New().String()
	}
	mq.peerName = peerName // routing key

	// declare queue, it will create queue only if it doesn't exist
	mq.queueP2P = mq.enusureQueue(
		fmt.Sprintf("oraksil.mq.q-p2p-%s", mq.peerName))

	mq.queueBroadcast = mq.enusureQueue(
		fmt.Sprintf("oraksil.mq.q-broadcast-%s", mq.peerName))

	// bind the queue with p2p exchange and broadcast one respectively
	mq.bindQueue(mq.queueP2P, mq.exchangeP2P, mq.peerName)
	mq.bindQueue(mq.queueBroadcast, mq.exchangeBroadcast, "")

	// start consuming
	go mq.consumerP2PQueue()
	go mq.consumerBroadcastQueue()

	// message handler
	go mq.messageHandler()

	// wait forever
	wg := &sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()
}

func (mq *MqService) consumerP2PQueue() {
	for {
		msgs, _ := mq.channel.Consume(
			mq.queueP2P.Name, // queue
			"",               // consumer
			true,             // auto-ack
			false,            // exclusive
			false,            // no-local
			false,            // no-wait
			nil,              // args
		)

		for m := range msgs {
			var recv Message
			mapstructure.Decode(m, &recv)

			if recv.Type == MSG_TYPE_RESERVED_REPLY {
				if _, ok := mq.recvMsgChannelsRpc[recv.MessageId]; !ok {
					panic("p2p channel must be created at sending.")
				}
				mq.recvMsgChannelsRpc[m.MessageId] <- recv
			} else {
				mq.recvMsgChannel <- recv
			}
		}
	}
}

func (mq *MqService) consumerBroadcastQueue() {
	for {
		msgs, _ := mq.channel.Consume(
			mq.queueBroadcast.Name, // queue
			"",                     // consumer
			true,                   // auto-ack
			false,                  // exclusive
			false,                  // no-local
			false,                  // no-wait
			nil,                    // args
		)

		for m := range msgs {
			var recvMsg Message
			mapstructure.Decode(m, &recvMsg)
			mq.recvMsgChannel <- recvMsg
		}
	}

}

func (mq *MqService) messageHandler() {
	func() {
		for msg := range mq.recvMsgChannel {
			if handlerFunc, ok := mq.handlerFuncs[msg.Type]; ok {
				retPayload := handlerFunc(&Context{message: msg})
				if msg.ReplyTo != "" {
					mq.reply(mq.exchangeP2P, msg, retPayload)
				}
			}
		}
	}()
}

func (mq *MqService) enusureQueue(name string) *amqp.Queue {
	q, err := mq.channel.QueueDeclare(
		name,  // name
		false, // durable
		false, // delete when unused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		panic(err)
	}
	return &q
}

func (mq *MqService) bindQueue(q *amqp.Queue, exchange string, routingKey string) {
	err := mq.channel.QueueBind(q.Name, routingKey, exchange, false, nil)
	if err != nil {
		panic(err)
	}
}

func (mq *MqService) fireAndForget(routingKey string, msgType string, payload interface{}) {
	var exchange string
	if routingKey != "" {
		exchange = mq.exchangeP2P
	} else {
		exchange = mq.exchangeBroadcast
	}

	mq.channel.Publish(
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		composeMessage("", msgType, "", payload),
	)
}

func (mq *MqService) sendAndWaitReply(routingKey string, msgType string, payload interface{}) Message {
	msg := composeMessage("", msgType, mq.peerName, payload)

	instantChannel := make(chan Message)
	mq.recvMsgChannelsRpc[msg.MessageId] = instantChannel

	mq.channel.Publish(
		mq.exchangeP2P,
		routingKey,
		false, // mandatory
		false, // immediate
		msg,
	)

	recvMsg := receiveMessageWithTimeout(instantChannel, 5)
	delete(mq.recvMsgChannelsRpc, msg.MessageId)
	close(instantChannel)

	return recvMsg
}

func (mq *MqService) reply(exchange string, origMsg Message, payload interface{}) {
	mq.channel.Publish(
		exchange,
		origMsg.ReplyTo,
		false, // mandatory
		false, // immediate
		composeMessage(origMsg.MessageId, "", "", payload),
	)
}

func (mq *MqService) AddHandler(handler MessageHandler) {
	mq.handlers = append(mq.handlers, handler)

	for _, r := range handler.Routes() {
		if _, ok := mq.handlerFuncs[r.MsgType]; ok {
			panic(fmt.Sprintf("handler for %s already exists.", r.MsgType))
		}

		mq.handlerFuncs[r.MsgType] = r.Handler
	}
}

func NewMqService(url, exchangeP2P, exchangeBroadcast string) *MqService {
	conn, err := amqp.Dial(url)
	if err != nil {
		panic(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	return &MqService{
		channel:            ch,
		exchangeP2P:        exchangeP2P,
		exchangeBroadcast:  exchangeBroadcast,
		recvMsgChannel:     make(chan Message, 1024),
		recvMsgChannelsRpc: make(map[string]chan Message),
		handlerFuncs:       make(map[string]HandlerFunc),
	}
}
