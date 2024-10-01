package main

import (
	"os"

	"github.com/IBM/sarama"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/config"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	configuration := config.New()

	kafkaConfig := config.GetKafkaConfig(configuration.Get("KAFKA_USERNAME"), configuration.Get("KAFKA_PASSWORD"))
	consumers, err := sarama.NewConsumer([]string{configuration.Get("KAFKA_URL")}, kafkaConfig)

	if err != nil {
		log.Error().Msgf("Failed to connect to Kafka: %v", err)
		os.Exit(1)
	}

	defer func() {
		if err := consumers.Close(); err != nil {
			log.Error().Msgf("Failed to close consumer: %v", err)
			return
		}
	}()

	// Setup Repository

}
