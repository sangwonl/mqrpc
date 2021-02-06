# mqrpc

Very simple RPC framework based on message queue system.

* It supports AMQP only for now. (such like [RabbitMQ](https://www.rabbitmq.com/))

## Get Package
```bash
$ go get github.com/sangwonl/mqrpc
```

## How to use
### Import Package
```go
import "github.com/sangwonl/mqrpc"
```

### Creating `MqService`
```go
const AmqpURI = "amqp://mqrpc:mqrpc@localhost:5672/"
const Namespace = "examples.rpc"
const PeerName = "myPeerName"

svc, err := mqrpc.NewMqService(AmqpURI, Namespace, PeerName)
if err != nil {
	panic(err)
}
```

### Adding Message Handler
```go
svc.AddHandler("someMsgType", func(ctx *mqrpc.Context) interface{} {
    var msgPayload SomeMsgPayload
    json.Unmarshal(ctx.GetMessage().Payload, &msgPayload)

    return nil

    // If you have something to return to client
    // return AnotherMsgPayload{}
})
```

### Run Service
```go
svc.Run(false)

// If you want to use SendToAny(),
// `enableWorker` flag must be set to true at server(worker) side.
// svc.Run(true)
``` 

### Call RPC
```go
msgClient := mqrpc.DefaultMessageService{MqService: svc}

args := SomeMsgPayload{hello: "world"}
resp, _ := msgClient.Request("myPeerName", "someMsgType", &args, 0)

var result Result
json.Unmarshal(resp.Payload, &result)
```

## Example

You can find more details from `/examples`.

For trying to run example, you might need to run rabbitmq instance.

```bash
docker run -d \
    --name mqrpc \
    --hostname mqrpc \
    -e RABBITMQ_DEFAULT_USER=mqrpc \
    -e RABBITMQ_DEFAULT_PASS=mqrpc \
    -p 5672:5672 \
    -p 15672:15672 \
    rabbitmq:3.8.5-management
```