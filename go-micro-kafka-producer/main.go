package main

import (
	"fmt"
	"os"
	"time"

	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/config"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/controller"
	kafkaconfig "github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/kafkaconfig"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/repository"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/service"
	"github.com/robfig/cron"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	configuration := config.New()

	// Set up Kafka connection
	producer, err := setupKafka(configuration)

	if err != nil {
		os.Exit(1)
	}
	// No need to close the producer as it does not have a Close method

	// Initialize the services and controller
	_, mainController := initializeServices(configuration, producer)

	// Start the cron job
	startCronJob(configuration, mainController)

	select {}
}

func setupKafka(configuration config.Config) (kafkaconfig.KafkaProducer, error) {
	brokersUrl := []string{configuration.Get("KAFKA_URL")}
	maxRetries := 5
	retryInterval := 30 * time.Second

	producer, err := config.RetryKafkaConnection(brokersUrl, maxRetries, retryInterval)
	if err != nil {
		log.Error().Msg("Failed to connect to Kafka after multiple retries")
		return nil, err // Return the error instead of exiting
	}

	log.Info().Msg("Kafka connected")
	return kafkaconfig.NewSaramaProducer(producer), nil
}

func initializeServices(configuration config.Config, kafkaProducer kafkaconfig.KafkaProducer) (service.MainService, *controller.MainController) {
	mainRepository := repository.NewMainRepository()
	mainService := service.NewMainService(&configuration, kafkaProducer, mainRepository)
	mainController := controller.NewMainController(mainService)
	return mainService, mainController
}

func startCronJob(configuration config.Config, mainController *controller.MainController) {
	c := cron.New()

	c.AddFunc(configuration.Get("JOB_MAIN_EXECTIME"), func() {
		log.Info().Msg("Exec Publish Message Main")
		now := time.Now()
		day := now.Weekday()
		fmt.Println(day)
		hour, _, _ := now.Clock()
		if day == time.Saturday || day == time.Sunday {
			log.Warn().Msg("Weekend, why are you still working?")
		} else if hour >= 8 && hour <= 17 {
			log.Info().Msg("Exec Publish Message Main")
			mainController.PublishMessageMain()
		}
	})
	c.Start()
}
