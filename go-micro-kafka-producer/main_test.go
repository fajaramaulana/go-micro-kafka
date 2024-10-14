package main_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/config"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/controller"
	kafkaconfig "github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/kafkaconfig"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/repository"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/service"
	"github.com/robfig/cron"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockKafkaProducer is a mock implementation of KafkaProducer
type MockKafkaProducer struct {
	mock.Mock
}

func (m *MockKafkaProducer) SendMessage(topic string, message []byte) error {
	args := m.Called(topic, message)
	return args.Error(0)
}

func TestSetupKafka(t *testing.T) {
	configuration := config.New() // Use a mock configuration if possible
	producer := setupKafka(configuration)

	assert.NotNil(t, producer)
}

func TestInitializeServices(t *testing.T) {
	configuration := config.New()
	mockProducer := new(MockKafkaProducer)

	mainService, mainController := initializeServices(configuration, mockProducer)
	assert.NotNil(t, mainService)
	assert.NotNil(t, mainController)
}

func TestStartCronJob(t *testing.T) {
	configuration := config.New()
	mockProducer := new(MockKafkaProducer)
	mainRepository := repository.NewMainRepository()
	mainService := service.NewMainService(&configuration, mockProducer, mainRepository)
	mainController := controller.NewMainController(mainService)

	// This will ensure that the cron job is added without running indefinitely
	startCronJob(configuration, mainController)
}

func setupKafka(configuration config.Config) kafkaconfig.KafkaProducer {
	brokersUrl := []string{configuration.Get("KAFKA_URL")}
	maxRetries := 5
	retryInterval := 30 * time.Second

	producer, err := config.RetryKafkaConnection(brokersUrl, maxRetries, retryInterval)
	if err != nil {
		log.Error().Msg("Failed to connect to Kafka after multiple retries")
		os.Exit(1)
	}

	log.Info().Msg("Kafka connected")
	return kafkaconfig.NewSaramaProducer(producer)
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
