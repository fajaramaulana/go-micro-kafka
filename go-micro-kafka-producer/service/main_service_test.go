package service_test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/config"
	kafkaconfigmock "github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/mock"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/model/response"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/service"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestPublishQueueMain(t *testing.T) {
	// Arrange: Set up mocks for KafkaProducer and MainRepository
	mockKafkaProducer := new(kafkaconfigmock.MockKafkaProducer)
	mockRepository := new(kafkaconfigmock.MockMainRepository)
	configuration := config.New("../.env") // Mock or use real config if needed

	// Create a sample data response for the test
	mockData := &response.GetData{
		Status:  true,
		Message: "Success",
		Time:    time.Now().String(),
		Data: []response.DataDetail{
			{Uuid: uuid.New().String(), Name: "Sample Data 1", Age: 20},
			{Uuid: uuid.New().String(), Name: "Sample Data 2", Age: 20},
		},
	}

	// Mock repository's GetData method
	mockRepository.On("GetData").Return(mockData, nil)

	// Mock KafkaProducer's SendMessage method, allowing any []byte
	mockKafkaProducer.On("SendMessage", "main_topic", mock.AnythingOfType("[]uint8")).Return(nil)

	// Set up service with mocks
	service := service.NewMainService(&configuration, mockKafkaProducer, mockRepository)

	// Act: Call the PublishQueueMain method
	service.PublishQueueMain()

	// Assert: Check if SendMessage was called and if the repository was used
	mockKafkaProducer.AssertExpectations(t)
	mockRepository.AssertExpectations(t)

	// You can also check that the correct topic and message are passed
	mockKafkaProducer.AssertCalled(t, "SendMessage", "main_topic", mock.Anything)
}

func TestPublishQueueMain_NilData(t *testing.T) {
	mockKafkaProducer := new(kafkaconfigmock.MockKafkaProducer)
	mockRepository := new(kafkaconfigmock.MockMainRepository)
	configuration := config.New("../.env") // Mock or use real config if needed

	// Mock repository to return nil data
	mockRepository.On("GetData").Return(nil, nil)

	// Create the service
	service := service.NewMainService(&configuration, mockKafkaProducer, mockRepository)

	// Call method under test
	service.PublishQueueMain()

	// Verify that SendMessage was not called
	mockKafkaProducer.AssertNotCalled(t, "SendMessage")
	mockRepository.AssertExpectations(t)
}

func TestPublishQueueMain_ErrorInRepository(t *testing.T) {

	mockKafkaProducer := new(kafkaconfigmock.MockKafkaProducer)
	mockRepository := new(kafkaconfigmock.MockMainRepository)
	configuration := config.New("../.env") // Mock or use real config if needed

	// Set up mock repository to return an error
	mockRepository.On("GetData").Return(nil, errors.New("database error"))

	service := service.NewMainService(&configuration, mockKafkaProducer, mockRepository)

	// Call method and verify error handling
	service.PublishQueueMain()

	// Verify the error handling is executed (logging, not sending message, etc.)
	mockKafkaProducer.AssertNotCalled(t, "SendMessage", mock.Anything, mock.Anything)
}

func TestPublishQueueMain_ErrorInKafkaProducer(t *testing.T) {

	mockKafkaProducer := new(kafkaconfigmock.MockKafkaProducer)
	mockRepository := new(kafkaconfigmock.MockMainRepository)
	configuration := config.New("../.env") // Mock or use real config if needed

	// Create a sample data response for the test
	mockData := &response.GetData{
		Status:  true,
		Message: "Success",
		Time:    time.Now().String(),
		Data: []response.DataDetail{
			{Uuid: uuid.New().String(), Name: "Sample Data 1", Age: 20},
			{Uuid: uuid.New().String(), Name: "Sample Data 2", Age: 20},
		},
	}

	// Mock repository's GetData method
	mockRepository.On("GetData").Return(mockData, nil)

	// Mock KafkaProducer's SendMessage method to return an error
	mockKafkaProducer.On("SendMessage", "main_topic", mock.AnythingOfType("[]uint8")).Return(errors.New("kafka error"))

	service := service.NewMainService(&configuration, mockKafkaProducer, mockRepository)

	// Call method and verify error handling
	service.PublishQueueMain()

	// Verify the error handling is executed (logging, not sending message, etc.)
	mockKafkaProducer.AssertCalled(t, "SendMessage", "main_topic", mock.Anything)
}

func TestPublishQueueMain_ErrorInSendingMessage(t *testing.T) {
	mockKafkaProducer := new(kafkaconfigmock.MockKafkaProducer)
	mockRepository := new(kafkaconfigmock.MockMainRepository)
	configuration := config.New("../.env") // Mock or use real config if needed

	// Set up mock data
	mockData := &response.GetData{
		Status:  true,
		Message: "Success",
		Time:    time.Now().String(),
		Data: []response.DataDetail{
			{Uuid: "1", Name: "Sample Data 1", Age: 20},
		},
	}

	// Mock repository to return data
	mockRepository.On("GetData").Return(mockData, nil)

	// Mock Kafka producer to return an error
	mockKafkaProducer.On("SendMessage", mock.Anything, mock.Anything).Return(errors.New("send error"))

	// Create the service
	service := service.NewMainService(&configuration, mockKafkaProducer, mockRepository)

	// Call method under test
	service.PublishQueueMain()

	// Verify that SendMessage was called
	mockKafkaProducer.AssertCalled(t, "SendMessage", mock.Anything, mock.Anything)
	mockRepository.AssertExpectations(t)
}

func TestSendMessageToKafka_Success(t *testing.T) {
	// Arrange: Set up mock producer and configuration
	mockKafkaProducer := new(kafkaconfigmock.MockKafkaProducer)
	configuration := config.New("../.env") // Mock or use real config if needed

	// Create the service with the mock config and repository
	mainService := service.NewMainService(&configuration, mockKafkaProducer, nil)

	// Set expectations for the mock configuration and producer
	topic := "main_topic"
	message := []byte("Test message")

	mockKafkaProducer.On("SendMessage", topic, message).Return(nil)

	// Act: Call the sendMessageToKafka function
	err := mainService.SendMessageToKafka(mockKafkaProducer, message)

	// Assert: Check that the message was sent successfully and mocks were called
	assert.NoError(t, err)
	mockKafkaProducer.AssertCalled(t, "SendMessage", topic, message)
}

func TestSendMessageToKafka_Failure(t *testing.T) {
	// Arrange: Set up mock producer and configuration
	mockKafkaProducer := new(kafkaconfigmock.MockKafkaProducer)
	configuration := config.New("../.env") // Mock or use real config if needed

	// Create the service with the mock config and repository
	mainService := service.NewMainService(&configuration, mockKafkaProducer, nil)

	// Set expectations for the mock configuration and producer
	topic := "main_topic"
	message := []byte("Test message")
	mockError := fmt.Errorf("Kafka producer error")

	mockKafkaProducer.On("SendMessage", topic, message).Return(mockError)

	// Act: Call the sendMessageToKafka function
	err := mainService.SendMessageToKafka(mockKafkaProducer, message)

	// Assert: Check that the error was returned and mocks were called
	assert.Error(t, err)
	assert.Equal(t, mockError, err)
	mockKafkaProducer.AssertCalled(t, "SendMessage", topic, message)
}

func TestSendMessageToKafka_NilMessage(t *testing.T) {
	// Create a new instance of the mock Kafka producer
	mockKafkaProducer := new(kafkaconfigmock.MockKafkaProducer)

	// Load your configuration
	configuration := config.New("../.env")

	// Create a new MainService instance
	mainService := service.NewMainService(&configuration, mockKafkaProducer, nil)

	// Define what should happen when SendMessage is called with a nil message
	mockKafkaProducer.On("SendMessage", "main_topic", mock.Anything).Return(errors.New("send error"))

	// Call the method under test
	err := mainService.SendMessageToKafka(mockKafkaProducer, nil)

	// Assert that the error is as expected
	assert.Error(t, err)                    // Expecting an error for nil message
	mockKafkaProducer.AssertExpectations(t) // Ensure all expectations were met
}

func TestSendMessageToKafka(t *testing.T) {
	// Create a new instance of the mock MainService
	mockService := new(kafkaconfigmock.MockMainService)

	// Create a mock producer (adjust this as per your actual producer mock)
	mockProducer := new(kafkaconfigmock.MockKafkaProducer) // Assuming you have a KafkaProducer mock

	tests := []struct {
		name    string
		message []byte
		wantErr bool
	}{
		{"Valid message", []byte("Test message"), false},
		{"Nil message", nil, true},
		{"Empty message", []byte(""), true}, // Expecting no error for empty message
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup the expected call based on the test case
			if tt.wantErr {
				mockService.On("SendMessageToKafka", mockProducer, mock.Anything).Return(errors.New("send error"))
			} else {
				mockService.On("SendMessageToKafka", mockProducer, tt.message).Return(nil) // Expecting nil for empty message
			}

			// Call the method under test
			err := mockService.SendMessageToKafka(mockProducer, tt.message)

			// Check the error
			if tt.wantErr {
				assert.Error(t, err) // Expecting an error for nil message
			} else {
				assert.NoError(t, err) // Expecting no error for valid and empty messages
			}

			// Assert that the expectations were met
			mockService.AssertExpectations(t)
		})
	}
}

func TestChunkData(t *testing.T) {
	uuidString1 := uuid.New().String()
	uuidString2 := uuid.New().String()
	uuidString3 := uuid.New().String()
	uuidString4 := uuid.New().String()

	tests := []struct {
		name      string
		data      []response.DataDetail
		chunkSize int
		expected  [][]response.DataDetail
	}{
		{
			name:      "Empty data",
			data:      []response.DataDetail{},
			chunkSize: 10,                        // Just specify chunkSize as a value here
			expected:  [][]response.DataDetail{}, // Expected result
		},
		{
			name: "Single chunk",
			data: []response.DataDetail{
				{Uuid: uuidString1, Name: "Data1", Age: 20},
				{Uuid: uuidString2, Name: "Data2", Age: 20},
			},
			chunkSize: 2,
			expected: [][]response.DataDetail{
				{
					{Uuid: uuidString1, Name: "Data1", Age: 20},
					{Uuid: uuidString2, Name: "Data2", Age: 20},
				},
			},
		},
		{
			name: "Multiple chunks",
			data: []response.DataDetail{
				{Uuid: uuidString1, Name: "Data1", Age: 20},
				{Uuid: uuidString2, Name: "Data2", Age: 20},
				{Uuid: uuidString3, Name: "Data3", Age: 20},
				{Uuid: uuidString4, Name: "Data4", Age: 20},
			},
			chunkSize: 2,
			expected: [][]response.DataDetail{
				{
					{Uuid: uuidString1, Name: "Data1", Age: 20},
					{Uuid: uuidString2, Name: "Data2", Age: 20},
				},
				{
					{Uuid: uuidString3, Name: "Data3", Age: 20},
					{Uuid: uuidString4, Name: "Data4", Age: 20},
				},
			},
		},
		{
			name: "Last chunk smaller than chunk size",
			data: []response.DataDetail{
				{Uuid: uuidString1, Name: "Data1", Age: 20},
				{Uuid: uuidString2, Name: "Data2", Age: 20},
				{Uuid: uuidString3, Name: "Data3", Age: 20},
			},
			chunkSize: 2,
			expected: [][]response.DataDetail{
				{
					{Uuid: uuidString1, Name: "Data1", Age: 20},
					{Uuid: uuidString2, Name: "Data2", Age: 20},
				},
				{
					{Uuid: uuidString3, Name: "Data3", Age: 20},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			result := service.ChunkData(tt.data, tt.chunkSize)
			if len(tt.expected) == 0 {
				assert.Empty(t, result) // Use assert.Empty for empty slices
			} else {
				assert.Equal(t, tt.expected, result) // Assert for non-empty slices
			}
		})
	}
}

func TestChunkData_LessThanChunkSize(t *testing.T) {
	uuidString1 := uuid.New().String()

	tests := []struct {
		name      string
		data      []response.DataDetail
		chunkSize int
		expected  [][]response.DataDetail
	}{
		{
			name: "Last chunk smaller than chunk size",
			data: []response.DataDetail{
				{Uuid: uuidString1, Name: "Data1", Age: 20},
			},
			chunkSize: 2,
			expected: [][]response.DataDetail{
				{
					{Uuid: uuidString1, Name: "Data1", Age: 20},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			result := service.ChunkData(tt.data, tt.chunkSize)
			if len(tt.expected) == 0 {
				assert.Empty(t, result) // Use assert.Empty for empty slices
			} else {
				assert.Equal(t, tt.expected, result) // Assert for non-empty slices
			}
		})
	}
}
