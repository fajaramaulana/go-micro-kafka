package kafkaconfigmock

import (
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/model/response"
	"github.com/stretchr/testify/mock"
)

// MockMainRepository is a mock implementation of MainRepository
type MockMainRepository struct {
	mock.Mock
}

// GetData is a mock method that simulates fetching data
func (m *MockMainRepository) GetData() (*response.GetData, error) {
	args := m.Called()
	if args.Get(0) != nil {
		return args.Get(0).(*response.GetData), args.Error(1)
	}
	return nil, args.Error(1)
}
