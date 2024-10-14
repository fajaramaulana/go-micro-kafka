package main

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/IBM/sarama"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/config"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/mocks"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/model/request"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
)

// TestConsumeClaim tests the ConsumeClaim method of ConsumerClaimPbk
func TestConsumeClaim(t *testing.T) {
	// Mock the MainService
	mockMainService := new(mocks.MockMainService)

	// Create a ConsumerClaimPbk with the mocked MainService
	consumer := &ConsumerClaimPbk{
		mainService: mockMainService,
		ready:       make(chan bool),
	}

	// Create a mock ConsumerGroupSession
	mockSession := new(mocks.MockConsumerGroupSession)

	// Create a sample message that would come from Kafka
	data := request.DataFromKafka{
		Data: []request.DataDetail{{
			Uuid: uuid.New().String(),
			Name: "John Doe",
			Age:  30,
		}},
	}
	messageBytes, _ := json.Marshal(data)

	// Mock a sarama.ConsumerMessage
	msg := &sarama.ConsumerMessage{
		Value: messageBytes,
	}

	// Expect MainFuncService to be called with the data
	mockMainService.On("MainFuncService", data.Data).Return()

	// Expect MarkMessage to be called after processing the message
	mockSession.On("MarkMessage", msg, "").Return()

	// Create a mock claim
	mockClaim := new(mocks.MockConsumerGroupClaim)
	mockClaim.On("Messages").Return(mockMessageChannel(msg))

	// Call the ConsumeClaim method
	err := consumer.ConsumeClaim(mockSession, mockClaim)
	assert.NoError(t, err)

	// Verify expectations
	mockMainService.AssertExpectations(t)
	mockSession.AssertExpectations(t)
	mockClaim.AssertExpectations(t)
}

func TestConsumeClaim_ErrorUMarshal(t *testing.T) {
	// Setup logger for test
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	// Create a mock service
	mockService := new(mocks.MockMainService)

	// Create the consumer
	consumer := &ConsumerClaimPbk{
		mainService: mockService,
		ready:       make(chan bool),
	}

	// Create a mock ConsumerGroupSession
	mockSession := new(mocks.MockConsumerGroupSession)

	// Create a message that cannot be unmarshalled
	badMessage := &sarama.ConsumerMessage{
		Value: []byte("{bad json}"),
	}

	// Create a mock ConsumerGroupClaim to simulate the message
	mockClaim := new(mocks.MockConsumerGroupClaim)
	mockClaim.On("Messages").Return(func() <-chan *sarama.ConsumerMessage {
		// Simulate sending the bad message in a goroutine to avoid blocking
		ch := make(chan *sarama.ConsumerMessage, 1)
		ch <- badMessage
		close(ch)
		return ch
	}())

	// Call the method being tested
	err := consumer.ConsumeClaim(mockSession, mockClaim)

	// Assertions
	assert.NoError(t, err)
	mockClaim.AssertExpectations(t)
	mockSession.AssertExpectations(t)
}

func TestConsumeClaim_LoadLocationNil(t *testing.T) {
	// Set up logger for test
	zerolog.SetGlobalLevel(zerolog.DebugLevel)

	// Create a mock service
	mockService := new(mocks.MockMainService)

	// Create the consumer
	consumer := &ConsumerClaimPbk{
		mainService: mockService,
		ready:       make(chan bool),
	}

	// Create a mock ConsumerGroupSession
	mockSession := new(mocks.MockConsumerGroupSession)

	// Create a mock ConsumerGroupClaim
	mockClaim := new(mocks.MockConsumerGroupClaim)

	// Create a read-only channel and put the message in it
	messageChan := make(chan *sarama.ConsumerMessage, 1)
	messageChan <- &sarama.ConsumerMessage{
		Value: []byte(`{"data": []}`), // Simulate a valid JSON message
	}
	close(messageChan)

	// Define what the Messages() method should return
	mockClaim.On("Messages").Return((<-chan *sarama.ConsumerMessage)(messageChan))
	// fmt.Printf("%# v\n", time.LoadLocation)
	// Patch time.LoadLocation to return an error
	monkey.Patch(time.LoadLocation, func(name string) (*time.Location, error) {
		return nil, assert.AnError
	})
	defer monkey.Unpatch(time.LoadLocation)

	// Set up a mock logger to capture the log output
	var logOutput bytes.Buffer
	log.Logger = log.Output(&logOutput)

	// Call the method being tested
	err := consumer.ConsumeClaim(mockSession, mockClaim)

	// Assertions
	assert.NoError(t, err) // The method should not return an error
	mockClaim.AssertExpectations(t)
	mockSession.AssertExpectations(t)

	// Check if the log contains the expected error message for nil location
	assert.Contains(t, logOutput.String(), "Error loading location")
}

func TestSetup(t *testing.T) {
	// Create a new instance of the consumer
	consumer := &ConsumerClaimPbk{
		ready: make(chan bool),
	}

	// Call the Setup method
	err := consumer.Setup(new(mocks.MockConsumerGroupSession))

	// Assertions
	assert.NoError(t, err, "Expected no error from Setup")
	// Check if the ready channel is closed
	select {
	case <-consumer.ready:
		// The channel is closed, which is expected
	default:
		t.Error("Expected ready channel to be closed")
	}
}

func TestCleanup(t *testing.T) {
	// Create a new instance of the consumer
	consumer := &ConsumerClaimPbk{}

	// Call the Cleanup method
	err := consumer.Cleanup(new(mocks.MockConsumerGroupSession))

	// Assertions
	assert.NoError(t, err, "Expected no error from Cleanup")
}

func TestCreateConsumerGroup(t *testing.T) {
	// Mock configuration
	mockConfig := config.New()
	mockConfig.Set("KAFKA_URL", "localhost:9092")
	mockConfig.Set("KAFKA_GROUP", "test-group")

	// Assuming we have a function to set Kafka config
	mockKafkaConfig := config.GetKafkaConfig("", "")
	mockKafkaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest // Set expected value

	// Create a consumer group
	consumerGroup, err := createConsumerGroup(mockConfig)

	// Assertions
	assert.NoError(t, err, "Expected no error while creating consumer group")
	assert.NotNil(t, consumerGroup, "Expected consumer group to be created")
}

// mockMessageChannel creates a channel with a single message for testing
func mockMessageChannel(msg *sarama.ConsumerMessage) <-chan *sarama.ConsumerMessage {
	ch := make(chan *sarama.ConsumerMessage, 1)
	ch <- msg
	close(ch)
	return ch
}
