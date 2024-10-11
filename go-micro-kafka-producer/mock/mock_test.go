package kafkaconfigmock_test

import (
	"errors"
	"testing"

	kafkaconfigmock "github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/mock" // Adjust the import path as necessary
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/model/response"
	"github.com/stretchr/testify/assert"
)

// TestSendMessage tests the SendMessage method of MockKafkaProducer
func TestSendMessage(t *testing.T) {
	// Create an instance of MockKafkaProducer
	mockProducer := new(kafkaconfigmock.MockKafkaProducer)

	// Define the expected behavior of SendMessage
	topic := "main_topic"
	message := []byte("test message")
	mockProducer.On("SendMessage", topic, message).Return(nil) // Expect it to be called with topic and message, returning nil

	// Call the SendMessage method
	err := mockProducer.SendMessage(topic, message)

	// Assert that no error is returned
	assert.NoError(t, err)

	// Assert that the expectations were met
	mockProducer.AssertExpectations(t)
}

// TestSendMessageWithError tests the SendMessage method with an error scenario
func TestSendMessageWithError(t *testing.T) {
	mockProducer := new(kafkaconfigmock.MockKafkaProducer)

	// Define the expected behavior with an error return
	topic := "main_topic"
	message := []byte("test message")
	mockProducer.On("SendMessage", topic, message).Return(errors.New("send error")) // Expect it to return an error

	// Call the SendMessage method
	err := mockProducer.SendMessage(topic, message)

	// Assert that the expected error is returned
	assert.EqualError(t, err, "send error")

	// Assert that the expectations were met
	mockProducer.AssertExpectations(t)
}

// TestGetData tests the GetData method of MockMainRepository
func TestGetData_Success(t *testing.T) {
	// Create an instance of MockMainRepository
	mockRepo := new(kafkaconfigmock.MockMainRepository)

	// Prepare the expected response
	expectedData := &response.GetData{
		// Populate with expected fields
		// FieldName: value,
	}

	// Set up the expectation
	mockRepo.On("GetData").Return(expectedData, nil) // Expect GetData to be called and return the expected data and no error

	// Call the GetData method
	data, err := mockRepo.GetData()

	// Assert that no error is returned and the data matches the expected result
	assert.NoError(t, err)
	assert.Equal(t, expectedData, data)

	// Assert that the expectations were met
	mockRepo.AssertExpectations(t)
}

// TestGetData_Error tests the GetData method when an error occurs
func TestGetData_Error(t *testing.T) {
	mockRepo := new(kafkaconfigmock.MockMainRepository)

	// Set up the expectation to return an error
	mockRepo.On("GetData").Return(nil, errors.New("fetch error")) // Expect it to return an error

	// Call the GetData method
	data, err := mockRepo.GetData()

	// Assert that an error is returned and data is nil
	assert.EqualError(t, err, "fetch error")
	assert.Nil(t, data)

	// Assert that the expectations were met
	mockRepo.AssertExpectations(t)
}

// TestPublishQueueMain tests the PublishQueueMain method
func TestPublishQueueMain(t *testing.T) {
	mockService := new(kafkaconfigmock.MockMainService)

	// Set up the expectation
	mockService.On("PublishQueueMain").Return() // Expect PublishQueueMain to be called

	// Call the method
	mockService.PublishQueueMain()

	// Assert that the expectations were met
	mockService.AssertExpectations(t)
}

// TestSendMessageToKafka tests the SendMessageToKafka method
func TestSendMessageToKafka_Success(t *testing.T) {
	mockService := new(kafkaconfigmock.MockMainService)
	mockProducer := new(kafkaconfigmock.MockKafkaProducer) // Assuming you have a mock for KafkaProducer

	message := []byte("test message")

	// Set up the expectation for SendMessageToKafka
	mockService.On("SendMessageToKafka", mockProducer, message).Return(nil) // Expect no error

	// Call the method
	err := mockService.SendMessageToKafka(mockProducer, message)

	// Assert that no error is returned
	assert.NoError(t, err)

	// Assert that the expectations were met
	mockService.AssertExpectations(t)
}

// TestSendMessageToKafka_Error tests the SendMessageToKafka method for error cases
func TestSendMessageToKafka_Error(t *testing.T) {
	mockService := new(kafkaconfigmock.MockMainService)
	mockProducer := new(kafkaconfigmock.MockKafkaProducer)

	message := []byte("test message")

	// Set up the expectation to return an error
	mockService.On("SendMessageToKafka", mockProducer, message).Return(errors.New("send error"))

	// Call the method
	err := mockService.SendMessageToKafka(mockProducer, message)

	// Assert that an error is returned
	assert.EqualError(t, err, "send error")

	// Assert that the expectations were met
	mockService.AssertExpectations(t)
}
