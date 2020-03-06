package main

import (
	"context"
	"fmt"
	"github.com/labstack/echo"
	"github.com/labstack/echo/middleware"
	"github.com/segmentio/kafka-go"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/websocket"
	"time"
)

func changeRole(c echo.Context, selector *bool) error {
	websocket.Handler(func(ws *websocket.Conn) {
		defer ws.Close()
		for {
			log.Println("Handler")
			msg := ""
			err := websocket.Message.Receive(ws, &msg)
			if err != nil {
				c.Logger().Error(err)
			}
			fmt.Printf("%s\n", msg)
			switch msg {
			case "server1:producer":
				log.Println("Let's produce messages")
				*selector = true
			case "server2:producer":
				log.Println("Let's read messages")
				*selector = false
			}
		}

	}).ServeHTTP(c.Response(), c.Request())
	return nil
}

func main() {
	selector := false
	e := echo.New()

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Static("/", "public")
	e.GET("/ws", func(c echo.Context) error {
		changeRole(c, &selector)
		return nil
	})
	e.Logger.Fatal(e.Start(":1333"))

	topic := "my131313topic"
	partition := 0
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	tMes := time.NewTicker(time.Second * 10)
	for {
		select {
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
				log.Println("read")
				conn.SetReadDeadline(time.Now().Add(3 * time.Second))
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
				}
				batch.Close()
			}
		}
	}
	defer conn.Close()
}
