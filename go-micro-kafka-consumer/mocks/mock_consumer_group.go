package mocks

import (
	"context"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/mock"
)

type MockConsumerGroup struct {
	mock.Mock
}

func (m *MockConsumerGroup) Consume(ctx context.Context, topics []string, handler sarama.ConsumerGroupHandler) error {
	args := m.Called(ctx, topics, handler)
	return args.Error(0)
}

func (m *MockConsumerGroup) Close() error {
	return m.Called().Error(0)
}

func (m *MockConsumerGroup) Errors() <-chan error {
	args := m.Called()
	return args.Get(0).(<-chan error)
}

func (m *MockConsumerGroup) PauseAll() {
	m.Called()
}

func (m *MockConsumerGroup) ResumeAll() {
	m.Called()
}

func (m *MockConsumerGroup) MemberID() string {
	return ""
}

func (m *MockConsumerGroup) Topics() []string {
	return []string{}
}

func (m *MockConsumerGroup) Claims() map[string][]int32 {
	return map[string][]int32{}
}

func (m *MockConsumerGroup) Pause(map[string][]int32) {
	m.Called()
}

func (m *MockConsumerGroup) Resume(map[string][]int32) {
	m.Called()
}
