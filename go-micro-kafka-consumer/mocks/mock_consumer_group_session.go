package mocks

import (
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/mock"
)

// MockConsumerGroupSession is a mock for sarama.ConsumerGroupSession
type MockConsumerGroupSession struct {
	mock.Mock
	sarama.ConsumerGroupSession
}

// MarkMessage is a mock method to track message acknowledgment.
func (m *MockConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, metadata string) {
	m.Called(msg, metadata)
}
