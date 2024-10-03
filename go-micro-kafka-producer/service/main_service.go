package service

import (
	kafkaconfig "github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/kafkaconfig"
)

type MainService interface {
	PublishQueueMain()
	SendMessageToKafka(producer kafkaconfig.KafkaProducer, message []byte) error
}
