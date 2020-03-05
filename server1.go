package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
)

func main() {
	topic := "my13topic"
	partition := 0
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	defer conn.Close()
	for {
		batch := conn.ReadBatch(1, 300) // fetch 10KB min, 1MB max

		for {
			b := make([]byte, 10e3) // 10KB max per message
			_, err := batch.Read(b)
			if err != nil {
				break
			}
			fmt.Println(string(b))
		}

		batch.Close()
	}
}
