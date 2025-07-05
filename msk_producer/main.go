package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/joho/godotenv"
)

const ()

func main() {
	godotenv.Load("../.env")
	brokers := strings.Split(os.Getenv("MSK_BOOTSTRAPSTRING"), ",")
	kafkaTopic := os.Getenv("MSK_TOPIC")

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("無法建立 Kafka Producer: %v", err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Value: sarama.StringEncoder(fmt.Sprintf("Hello Kafka from Go! Message at %s", time.Now().Format(time.RFC3339))),
	}

	if partition, offset, err := producer.SendMessage(msg); err != nil {
		log.Printf("發送訊息失敗: %v", err)
	} else {
		fmt.Printf("訊息發送到分區 %d, 位移 %d\n", partition, offset)
	}
}
