package main

import (
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
	// Load configuration
	configuration := config.New()

	// Kafka configuration
	kafkaConfig := config.GetKafkaConfig("", "")
	kafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest // Start from the earliest offset

	// Create Kafka consumer
	consumers, err := sarama.NewConsumer([]string{configuration.Get("KAFKA_URL")}, kafkaConfig)
	if err != nil {
		log.Fatal().Msgf("Error creating Kafka consumer: %v", err)
	}
	defer func() {
		if err := consumers.Close(); err != nil {
			log.Fatal().Msgf("Failed to close Kafka consumer: %v", err)
		}
	}()

	// Set up repository and service
	mainRepository := repository.NewMainRepository(&configuration)
	mainService := service.NewMainService(&mainRepository, &configuration)

	// Set up signal handler
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)

	// Get Kafka topic and partitions
	topicPbk := []string{configuration.Get("KAFKA_TOPIC_MAIN")}
	chanMessagePbk := make(chan *sarama.ConsumerMessage, 256)

	// Consume messages from each partition
	for _, topic := range topicPbk {
		partitionList, err := consumers.Partitions(topic)
		if err != nil {
			log.Printf("Unable to get partitions for topic %s: %v", topic, err)
			continue
		}

		for _, partition := range partitionList {
			go consumeMessages(consumers, topic, partition, chanMessagePbk)
		}
	}

	log.Info().Msg("Kafka consumer waiting for messages...")

ConsumerLoop:
	for {
		select {
		case msg := <-chanMessagePbk:
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
			}

			log.Info().Msgf("Time: %v Total Data: %v", t.In(location), len(response.Data))
			mainService.MainFuncService(response.Data)

		case sig := <-signals:
			log.Info().Msgf("Received signal: %v. Shutting down...", sig)
			break ConsumerLoop
		}
	}
}

func consumeMessages(consumer sarama.Consumer, topic string, partition int32, ch chan<- *sarama.ConsumerMessage) {
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		log.Info().Msgf("Error starting partition consumer for topic %s, partition %d: %v", topic, partition, err)
		return
	}
	defer partitionConsumer.Close()

	for msg := range partitionConsumer.Messages() {
		ch <- msg
	}
}

func (consumer *ConsumerClaimPbk) Setup(sarama.ConsumerGroupSession) error {
	close(consumer.ready)
	return nil
}

func (consumer *ConsumerClaimPbk) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (consumer *ConsumerClaimPbk) ConsumeClaim(session sarama.ConsumerGroupSession, mainFunc sarama.ConsumerGroupClaim) error {
	for {
		select {
		case message := <-mainFunc.Messages():
			var request []request.DataDetail
			json.Unmarshal([]byte(string(message.Value)), &request)
			log.Info().Msgf("New Message from kafka, message Pengiriman Bukti Kepeserataan: %v", string(message.Value))
			consumer.mainService.MainFuncService(request)
			session.MarkMessage(message, "")
		case <-session.Context().Done():
			return nil
		}
	}
}

func toggleConsumptionFlow(client sarama.ConsumerGroup, isPaused *bool) {
	if *isPaused {
		client.ResumeAll()
		log.Info().Msg("Resuming consumption")
	} else {
		client.PauseAll()
		log.Info().Msg("Pausing consumption")
	}

	*isPaused = !*isPaused
}
