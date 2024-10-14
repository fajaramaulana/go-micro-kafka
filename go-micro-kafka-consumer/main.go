package main

import (
	"context"
	"encoding/json"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/config"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/model/request"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/repository"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/service"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type ConsumerClaimPbk struct {
	mainService service.MainService
	ready       chan bool
}

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	log.Info().Msg("Starting Kafka consumer...")

	configuration := config.New()
	consumerGroup, err := createConsumerGroup(configuration)
	if err != nil {
		log.Fatal().Msgf("Error creating Kafka consumer group: %v", err)
	}
	defer consumerGroup.Close()

	mainRepository := repository.NewMainRepository(&configuration)
	mainService := service.NewMainService(mainRepository, &configuration)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	startConsuming(consumerGroup, mainService, configuration)

	log.Info().Msg("Kafka consumer waiting for messages...")
	<-signals // Wait for signal to exit
	log.Info().Msg("Shutting down Kafka consumer...")
}

func createConsumerGroup(configuration config.Config) (sarama.ConsumerGroup, error) {
	kafkaConfig := config.GetKafkaConfig("", "")
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest // Start from the earliest offset

	return sarama.NewConsumerGroup([]string{configuration.Get("KAFKA_URL")}, configuration.Get("KAFKA_GROUP"), kafkaConfig)
}

func startConsuming(consumerGroup sarama.ConsumerGroup, mainService service.MainService, configuration config.Config) {
	consumer := &ConsumerClaimPbk{
		mainService: mainService,
		ready:       make(chan bool),
	}

	go func() {
		for {
			if err := consumerGroup.Consume(context.Background(), []string{configuration.Get("KAFKA_TOPIC_MAIN")}, consumer); err != nil {
				log.Error().Msgf("Error from consumer: %v", err)
			}
			consumer.ready = make(chan bool) // Re-initialize for the next loop
		}
	}()
}

func (consumer *ConsumerClaimPbk) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *ConsumerClaimPbk) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *ConsumerClaimPbk) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Info().Msgf("New Message from kafka, message: %v", string(msg.Value))

		var response request.DataFromKafka
		err := json.Unmarshal(msg.Value, &response)
		if err != nil {
			log.Error().Msgf("Error unmarshalling message: %v", err)
			continue
		}

		t := time.Now()
		location, err := time.LoadLocation("Asia/Jakarta")
		if err != nil {
			log.Error().Msgf("Error loading location: %v", err)
			continue // Handle the error as necessary
		}

		if location == nil {
			log.Error().Msg("Location is nil")
		} else {
			log.Info().Msgf("Time: %v Total Data: %v", t.In(location), len(response.Data))
		}

		consumer.mainService.MainFuncService(response.Data)

		// Commit the offset only after successful processing
		session.MarkMessage(msg, "")
	}
	return nil
}
