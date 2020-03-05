//package main
//
//import (
//	"context"
//	"fmt"
//	"github.com/segmentio/kafka-go"
//	log "github.com/sirupsen/logrus"
//)
//
//func main() {
//	log.Info("Kafka consumer started")
//	topic := "my-topic15"
//	partition := 0
//	selector := false
//	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
//	if err!=nil{
//		log.Error(err)
//	}
//	//file, err := os.Open("whoIsWho.txt")
//	//if err != nil{
//	//	fmt.Println("Unable to open file:", err)
//	//	os.Exit(1)
//	//}
//	//data := make([]byte, 64)
//	//amount, err:=file.Read(data)
//	//str:=string(data[:amount])
//	//if str=="server1:producer" || str=="server2:consumer"{
//	//	selector = true
//	//}
//	//file.Close()
//	//tMes := time.NewTicker(time.Second * 5)
//	//tRole := time.NewTicker(time.Second * 30)
//	message := ""
//	//reader := bufio.NewReader(os.Stdin)
//	//log.Println("Attempt to ReadBatch")
//	//conn.SetReadDeadline(time.Now().Add(10*time.Second))
//	//conn.SetWriteDeadline(time.Now().Add(10*time.Second))
//	batch := conn.ReadBatch(1, 1e6)
//	log.Println("ReadBatch success")
//	for{
//		//select {
//		//case <-tMes.C:
//		//	if selector {
//		//		_, err := conn.WriteMessages(
//		//			kafka.Message{Value: []byte(fmt.Sprintf("server1 say: %s", time.Now().String()))},
//		//		)
//		//		if err != nil {
//		//			log.Error(err)
//		//		}
//		//	}
//		//case <-tRole.C:
//		//	if selector {
//		//		message="server2:producer"
//		//		log.Println("Event on server1 :", message)
//		//		_, err := conn.WriteMessages(
//		//			kafka.Message{Value: []byte(message)},
//		//		)
//		//		if err != nil {
//		//			log.Error(err)
//		//		}
//		//
//		//	} else{
//		//		message="server1:producer"
//		//		log.Println("Event on server1 :", message)
//		//		_, err := conn.WriteMessages(
//		//			kafka.Message{Value: []byte(message)},
//		//		)
//		//		if err != nil {
//		//			log.Error(err)
//		//		}
//		//
//		//	}
//		//
//		//
//		//}
//		//if selector{
//		//	log.Println("Введите сообщение:")
//		//	m, _, _ := reader.ReadLine()
//		//	message = string(m)
//		//}
//		//if selector{
//		//	_, err:= conn.WriteMessages(
//		//		kafka.Message{Value: []byte(message)},
//		//		kafka.Message{Value: []byte("by server1")},
//		//	)
//		//	if err!=nil{
//		//		log.Error(err)
//		//	}
//		//} else{
//
//		if !selector{
//			//log.Println("Attempt to ReadBatch")
//			//batch := conn.ReadBatch(1, 1e6)
//			//log.Println("ReadBatch success")
//			for {
//				b := make([]byte, 1e3) // 10KB max per message
//				amount, err := batch.Read(b)
//				if err != nil {
//					break
//				}
//				bb:=b[:amount]
//				mes := fmt.Sprintf("%s",bb)
//				fmt.Println(mes)
//				if mes=="server1:producer"{
//					log.Println("Event on server1 : selector->produce")
//					selector = true
//					break
//				}
//				if mes=="exit"{
//					return
//				}
//			}
//		}
//		if message=="server2:producer"{
//			log.Println("Event on server1 : selector->consume")
//			selector = false
//		}
//		if message=="exit"{
//			return
//		}
//
//
//	}
//	batch.Close()
//	defer conn.Close()
//}
