package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/config"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/controller"
	kafkaconfig "github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/kafkaconfig"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/repository"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/service"
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

// Mocked function for testing
func MockRetryKafkaConnectionFailure(brokers []string, maxRetries int, retryInterval time.Duration) (sarama.SyncProducer, error) {
	return nil, fmt.Errorf("simulated connection failure")
}

func TestSetupKafka(t *testing.T) {
	configuration := config.New() // Use a mock configuration if possible
	producer, _ := setupKafka(configuration)

	assert.NotNil(t, producer)
}

func TestSetupKafka_Error(t *testing.T) {
	configuration := config.New()
	configuration.Set("KAFKA_URL", "localhost:9091") // Invalid URL
	configuration.Set("KAFKA_TOPIC", "invalid-topic")
	configuration.Set("JOB_MAIN_EXECTIME", "@every 1s")

	producer, err := setupKafkaMock(configuration, MockRetryKafkaConnectionFailure)

	assert.Error(t, err)
	assert.Nil(t, producer)
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

// Update your setupKafka function to accept a retry function
func setupKafkaMock(configuration config.Config, retryFunc func([]string, int, time.Duration) (sarama.SyncProducer, error)) (kafkaconfig.KafkaProducer, error) {
	brokersUrl := []string{configuration.Get("KAFKA_URL")}
	maxRetries := 5
	retryInterval := 30 * time.Second

	producer, err := retryFunc(brokersUrl, maxRetries, retryInterval)
	if err != nil {
		log.Error().Msg("Failed to connect to Kafka after multiple retries")
		return nil, err
	}

	log.Info().Msg("Kafka connected")
	return kafkaconfig.NewSaramaProducer(producer), nil
}
