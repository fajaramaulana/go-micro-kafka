package service

import (
	"testing"

	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/config"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/model/request"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/repository"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type MockMainRepository struct {
	mock.Mock
}

var _ repository.MainRepository = (*MockMainRepository)(nil) // Ensure MockMainRepository implements MainRepository

func (m *MockMainRepository) MainFuncRepository(params *request.ParamsId) (string, error) {
	args := m.Called(params)
	return args.Get(0).(string), args.Error(1)
}

func TestNewMainService(t *testing.T) {
	// Set up a mock configuration
	configuration := config.New("../.env")

	// Initialize a mock repository
	mockRepo := new(MockMainRepository)

	// Create the service
	service := NewMainService(mockRepo, &configuration)

	// Assertions
	assert.NotNil(t, service, "Expected service to be created")
}

func TestMainFuncService(t *testing.T) {
	configuration := config.New("../.env") // Ensure New() returns *config.Config

	mockRepo := new(MockMainRepository)
	svc := NewMainService(mockRepo, &configuration) // Pass the mock directly

	// Prepare test data
	params := []request.DataDetail{
		{Uuid: "uuid-1"},
		{Uuid: "uuid-2"},
	}

	// Define expected calls and their return values
	for _, param := range params {
		paramsId := request.ParamsId{ID: param.Uuid}
		mockRepo.On("MainFuncRepository", &paramsId).Return("", nil).Once()
	}

	// Call the method being tested
	result := svc.MainFuncService(params)

	// Assert that the result is as expected (in this case, nil)
	assert.Nil(t, result)

	// Assert that all expectations were met
	mockRepo.AssertExpectations(t)
}

func TestMainFuncService_Error(t *testing.T) {
	configuration := config.New("../.env") // Ensure New() returns *config.Config

	mockRepo := new(MockMainRepository)
	svc := NewMainService(mockRepo, &configuration) // Pass the mock directly

	// Prepare test data
	params := []request.DataDetail{
		{Uuid: "uuid-1"},
		{Uuid: "uuid-2"},
	}

	// Define expected calls and their return values
	for _, param := range params {
		paramsId := request.ParamsId{ID: param.Uuid}
		mockRepo.On("MainFuncRepository", &paramsId).Return("", assert.AnError).Once()
	}

	// Call the method being tested
	result := svc.MainFuncService(params)

	// Assert that the result is as expected (in this case, nil)
	assert.Nil(t, result)

	// Assert that all expectations were met
	mockRepo.AssertExpectations(t)
}
