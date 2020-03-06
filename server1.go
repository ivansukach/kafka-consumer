package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"time"
)

func main() {
	topic := "my13313topic"
	partition := 0
	selector := false
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	//var ticker *time.Ticker
	tMes := time.NewTicker(time.Second * 500)
	tMes = time.NewTicker(time.Second * 10)
	ticker := time.NewTicker(30 * time.Second)
	for {
		select {
		case <-ticker.C:
			if selector {
				_, err := conn.WriteMessages(
					kafka.Message{Value: []byte(fmt.Sprintf("server2:producer"))},
				)
				if err != nil {
					log.Error(err)
				}
				selector = false
			}
		case <-tMes.C:
			if selector {
				log.Println("write")
				_, err := conn.WriteMessages(
					kafka.Message{Value: []byte(fmt.Sprintf("server1 say: %s", time.Now().UTC().String()))},
				)
				if err != nil {
					log.Error(err)
				}
			} else {
				batch := conn.ReadBatch(1, 3000) // fetch 10KB min, 1MB max
				for {
					b := make([]byte, 10e3) // 10KB max per message
					amount, err := batch.Read(b)
					if err != nil {
						break
					}
					bb := b[:amount]
					mes := string(bb)
					log.Println(mes)
					if mes == "server1:producer" {
						log.Println("Event on server1 : selector->produce")
						selector = true
						ticker = time.NewTicker(30 * time.Second)
						break
					}
				}
				batch.Close()
			}
		}
	}
	defer conn.Close()
}
