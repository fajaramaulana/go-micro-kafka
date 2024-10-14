package mocks

import (
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/mock"
)

// MockConsumerGroupClaim is a mock for sarama.ConsumerGroupClaim
type MockConsumerGroupClaim struct {
	mock.Mock
	sarama.ConsumerGroupClaim
}

// Messages returns the mocked message channel
func (m *MockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage {
	args := m.Called()
	return args.Get(0).(<-chan *sarama.ConsumerMessage)
}
