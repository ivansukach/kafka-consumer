package main

import (
	"context"
	"errors"
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
			}
		}

	}).ServeHTTP(c.Response(), c.Request())
	return nil
}

func main() {
	selector := false
	go messageExchange(&selector)
	e := echo.New()

	e.Use(middleware.Logger())
	e.Use(middleware.Recover())
	e.Static("/", "public")
	e.GET("/ws", func(c echo.Context) error {
		return changeRole(c, &selector)
	})
	e.Logger.Fatal(e.Start(":1333"))

}
func messageExchange(selector *bool) {
	//time.Sleep(100*time.Second)
	origin := "http://localhost/"
	url := "ws://localhost:1323/ws"
	err := errors.New("newError")
	var ws *websocket.Conn
	for err != nil { //Oleg says that this is bad way to connect
		ws, err = websocket.Dial(url, "", origin)
		time.Sleep(time.Second)
	}

	if err != nil {
		log.Fatal(err)
	}
	topic := "my100topic"
	partition := 0
	conn, _ := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	defer conn.Close()
	tMes := time.NewTicker(time.Second * 4)
	tRole := time.NewTicker(time.Second * 30)
	for {
		select {
		case <-tRole.C:
			if *selector {
				if _, err := ws.Write([]byte("server2:producer")); err != nil {
					log.Fatal(err)
				}
				log.Println("Let's read messages")
				*selector = false
			}
		case <-tMes.C:
			if *selector {
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

}
