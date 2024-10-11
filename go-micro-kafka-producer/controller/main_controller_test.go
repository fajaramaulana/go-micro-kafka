package controller_test

import (
	"testing"

	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/controller"
	kafkaconfigmock "github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/mock"
)

func TestPublishMessageMain(t *testing.T) {
	// Arrange: Set up a mock MainService
	mockService := new(kafkaconfigmock.MockMainService)
	controller := controller.NewMainController(mockService)

	// Expect the PublishQueueMain method to be called
	mockService.On("PublishQueueMain").Return()

	// Act: Call the controller method
	controller.PublishMessageMain()

	// Assert: Check if PublishQueueMain was called
	mockService.AssertExpectations(t)
	mockService.AssertCalled(t, "PublishQueueMain")
}
