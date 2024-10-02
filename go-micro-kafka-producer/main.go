package main

import (
	"fmt"
	"os"
	"time"

	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/config"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/controller"
	kafkaconfig "github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/kafka"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/repository"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/service"
	"github.com/robfig/cron"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	configuration := config.New()

	// Try to connect with retries
	brokersUrl := []string{configuration.Get("KAFKA_URL")}
	maxRetries := 5                  // Number of retry attempts
	retryInterval := 5 * time.Second // Start retry interval
	producer, err := config.RetryKafkaConnection(brokersUrl, maxRetries, retryInterval)
	if err != nil {
		log.Error().Msg("Failed to connect to Kafka after multiple retries")
		os.Exit(1)
	}
	defer producer.Close()
	log.Info().Msg("Kafka connected")
	log.Info().Msg("Starting Cron Job")

	// Wrap the sarama producer with SaramaProducer
	kafkaProducer := kafkaconfig.NewSaramaProducer(producer)

	// initialize repository
	mainRepository := repository.NewMainRepository()

	// initialize service with kafka producer
	mainService := service.NewMainService(&configuration, kafkaProducer, &mainRepository)
	// Initialize controller with Kafka producer
	mainController := controller.NewMainController(mainService)

	// Initialize cron schedulers
	c := cron.New()
	// Cron job
	c.AddFunc(configuration.Get("JOB_MAIN_EXECTIME"), func() {
		log.Info().Msg("Exec Publish Message Main")
		now := time.Now()
		day := now.Weekday()
		fmt.Println(day)
		hour, _, _ := now.Clock()
		if day == time.Saturday || day == time.Sunday {
			log.Warn().Msg("Weekend, why you still working?")
		} else {
			// execute every 5 minutes
			if hour >= 8 && hour <= 17 {
				log.Info().Msg("Exec Publish Message Main")
				mainController.PublishMessageMain()
			}
		}
	})
	c.Start()

	select {}

}
