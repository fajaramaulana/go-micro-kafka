package kafkaconfigmock

import "github.com/stretchr/testify/mock"

// MockKafkaProducer is a mock implementation of the KafkaProducer interface
type MockKafkaProducer struct {
	mock.Mock
}

// SendMessage is the mocked method for testing
func (m *MockKafkaProducer) SendMessage(topic string, message []byte) error {
	args := m.Called(topic, message)
	return args.Error(0)
}
