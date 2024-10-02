package main

import (
	"fmt"
	"os"
	"time"

	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/config"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/controller"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/repository"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/service"
	"github.com/robfig/cron"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	configuration := config.New()

	// setup repository
	mainRepository := repository.NewMainRepository()

	// setup service
	mainService := service.NewMainService(&configuration, &mainRepository)

	// setup controller
	mainController := controller.NewMainController(&mainService)
	// checking if kafka is connected
	brokersUrl := []string{configuration.Get("KAFKA_URL")}
	producer, err := config.ConnectProducer(brokersUrl)
	if err != nil {
		log.Error().Msg("Failed to connect to Kafka")
		os.Exit(1)
	}
	defer producer.Close()
	log.Info().Msg("Kafka connected")
	log.Info().Msg("Starting Cron Job")
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
