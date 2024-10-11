package kafkaconfigmock

import (
	kafkaconfig "github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/kafkaconfig"
	"github.com/stretchr/testify/mock"
)

// MockMainService is a mock implementation of the MainService interface
type MockMainService struct {
	mock.Mock
}

// PublishQueueMain is the mocked method for testing
func (m *MockMainService) PublishQueueMain() {
	m.Called() // Track if this function is called during the test
}

// SendMessageToKafka now includes more detailed error handling
func (m *MockMainService) SendMessageToKafka(producer kafkaconfig.KafkaProducer, message []byte) error {
	args := m.Called(producer, message) // Track if this function is called during the test
	return args.Error(0)                // Ensure that the appropriate error is returned
}
