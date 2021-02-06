package mqrpc

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/mitchellh/mapstructure"
	"github.com/segmentio/ksuid"
	"github.com/streadway/amqp"
)

type Message struct {
	ReplyTo   string    // application use - address to reply to (ex: RPC)
	MessageId string    // application use - message identifier
	Timestamp time.Time // application use - message timestamp
	Type      MsgType   // application use - message type name

	// UserId        string    // application use - creating user - should be authenticated user
	// AppId         string    // application use - creating application id
	// CorrelationId string    // application use - correlation identifier

	Payload []byte `mapstructure:"Body"`
}

type Context struct {
	message Message
}

func (c *Context) GetMessage() Message {
	return c.message
}

type MsgType string
type HandlerFunc func(ctx *Context) interface{}

const MsgTypeReservedReply MsgType = "reply"

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

	handlerFuncs map[MsgType]HandlerFunc
}

func (mq *MqService) Run(enableWorker bool) error {
	// start consuming
	go mq.consumerP2PQueue()
	go mq.consumerBroadcastQueue()

	if enableWorker {
		go mq.consumerWorkerQueue()
	}

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

		if recv.Type == MsgTypeReservedReply {
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

func (mq *MqService) fireAndForget(routingKey string, msgType MsgType, payload interface{}, broadcast bool) error {
	var exchange string

	if broadcast {
		exchange = mq.exchangeBroadcast
		routingKey = ""
	} else {
		if routingKey != "" {
			exchange = mq.exchangeP2P
		} else {
			exchange = ""
			routingKey = mq.queueWorker.Name
		}
	}

	return mq.channel.Publish(
		exchange,
		routingKey,
		false, // mandatory
		false, // immediate
		composeMessage(msgType, "", "", payload),
	)
}

func (mq *MqService) sendAndWaitReply(routingKey string, msgType MsgType, payload interface{}, timeout time.Duration) (*Message, error) {
	msg := composeMessage(msgType, "", mq.peerName, payload)

	instantChannel := make(chan Message)
	mq.recvMsgChannelsRpc.Store(msg.MessageId, instantChannel)

	err := mq.channel.Publish(
		mq.exchangeP2P,
		routingKey,
		false, // mandatory
		false, // immediate
		msg,
	)

	if err != nil {
		return nil, err
	}

	recvMsg := receiveMessageWithTimeout(instantChannel, timeout)
	mq.recvMsgChannelsRpc.Delete(msg.MessageId)
	close(instantChannel)

	return &recvMsg, nil
}

func (mq *MqService) reply(exchange string, origMsg Message, payload interface{}) {
	mq.channel.Publish(
		exchange,
		origMsg.ReplyTo,
		false, // mandatory
		false, // immediate
		composeMessage("", origMsg.MessageId, "", payload),
	)
}

func (mq *MqService) AddHandler(msgType MsgType, handler HandlerFunc) error {
	if _, ok := mq.handlerFuncs[msgType]; ok {
		return errors.New(fmt.Sprintf("handler for %s already exists.", msgType))
	}

	mq.handlerFuncs[msgType] = handler

	return nil
}

func NewMqService(amqpUri, namespace, peerName string) (*MqService, error) {
	conn, err := amqp.Dial(amqpUri)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	// generate unique peer name
	if peerName == "" {
		peerName = ksuid.New().String()
	}

	mq := MqService{
		peerName:           peerName, // routing key
		namespace:          namespace,
		channel:            ch,
		recvMsgChannelsRpc: sync.Map{},
		recvMsgChannel:     make(chan Message, 1023),
		handlerFuncs:       make(map[MsgType]HandlerFunc),
	}

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
		fmt.Sprintf("mqrpc.%s.q-worker", mq.namespace), false)

	// bind the queue with p2p exchange and broadcast one respectively
	mq.bindQueue(mq.queueP2P, mq.exchangeP2P, mq.peerName)
	mq.bindQueue(mq.queueBroadcast, mq.exchangeBroadcast, "")

	return &mq, nil
}
