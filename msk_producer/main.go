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
	err := godotenv.Load("../.env")
	if err != nil {
		log.Fatalf("載入 .env 檔案失敗，可能檔案不存在或讀取錯誤: %v", err)
		// 這裡不使用 log.Fatalf 是為了即使 .env 不存在，程式也能繼續執行
		// 這在生產環境中，變數可能直接從環境中設定時很有用
	}
	kafkaBroker := os.Getenv("MSK_BOOTSTRAPSTRING")
	brokers := strings.Split(kafkaBroker, ",")
	kafkaTopic := os.Getenv("MSK_TOPIC")

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("無法建立 Kafka Producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Fatalf("無法關閉 Kafka Producer: %v", err)
		}
	}()

	fmt.Printf("成功連接到 Kafka 代理伺服器: %s\n", kafkaBroker)

	value := fmt.Sprintf("Hello Kafka from Go! Message at %s", time.Now().Format(time.RFC3339))
	msg := &sarama.ProducerMessage{
		Topic: kafkaTopic,
		Value: sarama.StringEncoder(value),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		log.Printf("發送訊息失敗: %v", err)
	} else {
		fmt.Printf("訊息 '%s' 發送到分區 %d, 位移 %d\n", value, partition, offset)
	}
}
