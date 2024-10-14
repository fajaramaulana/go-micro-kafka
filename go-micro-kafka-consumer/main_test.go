package main

import (
	"encoding/json"
	"testing"

	"github.com/IBM/sarama"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/mocks"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/model/request"
	"github.com/google/uuid"
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

// mockMessageChannel creates a channel with a single message for testing
func mockMessageChannel(msg *sarama.ConsumerMessage) <-chan *sarama.ConsumerMessage {
	ch := make(chan *sarama.ConsumerMessage, 1)
	ch <- msg
	close(ch)
	return ch
}
