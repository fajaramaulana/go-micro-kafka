package mocks

import (
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/model/request"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/mock"
)

// MockMainService is a mock for the MainService interface
type MockMainService struct {
	mock.Mock
}

// MainFuncService is a mock method to simulate the processing of data
func (m *MockMainService) MainFuncService(data []request.DataDetail) *fiber.Map {
	m.Called(data)
	return nil
}
