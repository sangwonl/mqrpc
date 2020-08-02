package mqrpc

import (
	"encoding/json"
	"errors"
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

// type MessageService interface {
// 	Identifier() string
// 	AllParticipants() []string

// 	Send(to string, msgType string, payload interface{})
// 	Broadcast(msgType string, payload interface{})
// 	Request(to string, msgType string, payload interface{}) interface{}
// }

type DefaultMessageServiceImpl struct {
	MqService *MqService
}

func (m *DefaultMessageServiceImpl) Identifier() string {
	return m.MqService.peerName
}

func (m *DefaultMessageServiceImpl) Send(to, msgType string, payload interface{}) error {
	return m.MqService.fireAndForget(to, msgType, payload, false)
}

func (m *DefaultMessageServiceImpl) SendToAny(msgType string, payload interface{}) error {
	return m.MqService.fireAndForget("", msgType, payload, false)
}

func (m *DefaultMessageServiceImpl) Broadcast(msgType string, payload interface{}) error {
	return m.MqService.fireAndForget("", msgType, payload, true)
}

func (m *DefaultMessageServiceImpl) Request(to, msgType string, payload interface{}, timeout time.Duration) (interface{}, error) {
	return m.MqService.sendAndWaitReply(to, msgType, payload, timeout)
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
	namespace string
	peerName  string

	channel           *amqp.Channel
	queueP2P          *amqp.Queue
	queueBroadcast    *amqp.Queue
	queueWorker       *amqp.Queue
	exchangeP2P       string
	exchangeBroadcast string

	recvMsgChannelsRpc sync.Map
	recvMsgChannel     chan Message

	handlers     []MessageHandler
	handlerFuncs map[string]HandlerFunc
}

func (mq *MqService) Run(peerName string) error {
	// generate unique peer name
	if peerName == "" {
		peerName = ksuid.New().String()
	}
	mq.peerName = peerName // routing key

	// declare exchange, it will create only if it doesn't exist
	mq.exchangeP2P = mq.ensureExchange(
		fmt.Sprintf("mqrpc.%s.p2p", mq.namespace), "direct")
	mq.exchangeBroadcast = mq.ensureExchange(
		fmt.Sprintf("mqrpc.%s.broadcast", mq.namespace), "fanout")

	// declare queue, it will create queue only if it doesn't exist
	mq.queueP2P = mq.enusureQueue(
		fmt.Sprintf("mqrpc.%s.q-p2p-%s", mq.namespace, mq.peerName), true)
	mq.queueBroadcast = mq.enusureQueue(
		fmt.Sprintf("mqrpc.%s.q-broadcast-%s", mq.namespace, mq.peerName), true)
	mq.queueWorker = mq.enusureQueue(
		fmt.Sprintf("mqrpc.%s.q-worker-%s", mq.namespace, mq.peerName), false)

	// bind the queue with p2p exchange and broadcast one respectively
	mq.bindQueue(mq.queueP2P, mq.exchangeP2P, mq.peerName)
	mq.bindQueue(mq.queueBroadcast, mq.exchangeBroadcast, "")

	// start consuming
	go mq.consumerP2PQueue()
	go mq.consumerBroadcastQueue()
	go mq.consumerWorkerQueue()

	// message handler
	go mq.messageHandler()

	// wait forever
	wg := &sync.WaitGroup{}
	wg.Add(1)
	wg.Wait()

	return nil
}

func (mq *MqService) consumerP2PQueue() {
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
			if ch, ok := mq.recvMsgChannelsRpc.Load(recv.MessageId); ok {
				ch.(chan Message) <- recv
			}
		} else {
			mq.recvMsgChannel <- recv
		}
	}
}

func (mq *MqService) consumerBroadcastQueue() {
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

func (mq *MqService) consumerWorkerQueue() {
	msgs, _ := mq.channel.Consume(
		mq.queueWorker.Name, // queue
		"",                  // consumer
		true,                // auto-ack
		false,               // exclusive
		false,               // no-local
		false,               // no-wait
		nil,                 // args
	)

	for m := range msgs {
		var recvMsg Message
		mapstructure.Decode(m, &recvMsg)
		mq.recvMsgChannel <- recvMsg
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

func (mq *MqService) ensureExchange(name, excType string) string {
	err := mq.channel.ExchangeDeclare(
		name,    // name
		excType, // type
		false,   // durable
		false,   // auto-deleted
		false,   // internal
		false,   // no-wait
		nil,     // arguments
	)
	if err != nil {
		panic(err)
	}

	return name
}

func (mq *MqService) enusureQueue(name string, exclusive bool) *amqp.Queue {
	q, err := mq.channel.QueueDeclare(
		name,      // name
		false,     // durable
		false,     // delete when unused
		exclusive, // exclusive
		false,     // no-wait
		nil,       // arguments
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

func (mq *MqService) fireAndForget(routingKey string, msgType string, payload interface{}, broadcast bool) error {
	var exchange string

	if broadcast {
		exchange = mq.exchangeBroadcast
		routingKey = ""
	} else {
		if routingKey != "" {
			exchange = mq.exchangeP2P
		} else {
			exchange = mq.queueWorker.Name
		}
	}

	mq.channel.Publish(
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		composeMessage("", msgType, "", payload),
	)

	return nil
}

func (mq *MqService) sendAndWaitReply(routingKey string, msgType string, payload interface{}, timeout time.Duration) (interface{}, error) {
	msg := composeMessage("", msgType, mq.peerName, payload)

	instantChannel := make(chan Message)
	mq.recvMsgChannelsRpc.Store(msg.MessageId, instantChannel)

	mq.channel.Publish(
		mq.exchangeP2P,
		routingKey,
		false, // mandatory
		false, // immediate
		msg,
	)

	recvMsg := receiveMessageWithTimeout(instantChannel, timeout)
	mq.recvMsgChannelsRpc.Delete(msg.MessageId)
	close(instantChannel)

	var value map[string]interface{}
	json.Unmarshal(recvMsg.Payload, &value)

	return value, nil
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

func (mq *MqService) AddHandler(handler MessageHandler) error {
	mq.handlers = append(mq.handlers, handler)

	for _, r := range handler.Routes() {
		if _, ok := mq.handlerFuncs[r.MsgType]; ok {
			return errors.New(fmt.Sprintf("handler for %s already exists.", r.MsgType))
		}

		mq.handlerFuncs[r.MsgType] = r.Handler
	}

	return nil
}

func NewMqService(amqpUri, namespace string) (*MqService, error) {
	conn, err := amqp.Dial(amqpUri)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	return &MqService{
		namespace:          namespace,
		channel:            ch,
		recvMsgChannelsRpc: sync.Map{},
		recvMsgChannel:     make(chan Message, 1024),
		handlerFuncs:       make(map[string]HandlerFunc),
	}, nil
}
