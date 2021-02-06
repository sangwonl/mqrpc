package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/sangwonl/mqrpc"
)

type pingData struct {
	Peer string
	Ts   int64
}

func main() {
	const AmqpURI = "amqp://mqrpc:mqrpc@localhost:5672/"
	const Namespace = "examples.rpc"
	peerName := os.Args[1]

	svc, err := mqrpc.NewMqService(AmqpURI, Namespace, peerName)
	if err != nil {
		panic(err)
	}

	if peerName == "server" {
		svc.AddHandler("ping", func(ctx *mqrpc.Context) interface{} {
			var cliPing pingData
			json.Unmarshal(ctx.GetMessage().Payload, &cliPing)
			fmt.Printf("Received ping from %s: %d\n", cliPing.Peer, cliPing.Ts)

			return pingData{Peer: peerName, Ts: time.Now().Unix()}
		})
	} else {
		go func() {
			msgClient := mqrpc.DefaultMessageService{MqService: svc}

			for {
				time.Sleep(2 * time.Second)

				cliPing := pingData{Peer: peerName, Ts: time.Now().Unix()}
				resp, _ := msgClient.Request("server", "ping", &cliPing, 0)
				msg, _ := resp.(*mqrpc.Message)

				var srvPing pingData
				json.Unmarshal(msg.Payload, &srvPing)

				fmt.Printf("Received ping from %s: %d\n", srvPing.Peer, srvPing.Ts)
			}
		}()
	}

	svc.Run(false)
}
