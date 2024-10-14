package service

import (
	"testing"

	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/config"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/model/request"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/repository"
	"github.com/stretchr/testify/assert"
)

func TestNewMainService(t *testing.T) {
	// Set up a mock configuration
	configuration := config.New("../.env") // Assuming New() initializes a config object
	mainRepository := repository.NewMainRepository(&configuration)

	// Create the service
	service := NewMainService(&mainRepository, &configuration)

	// Assertions
	assert.NotNil(t, service, "Expected service to be created")
	// assert.Equal(t, mainRepository, service.MainRepository, "Expected MainRepository to be set correctly")
	// assert.Equal(t, configuration, service.Configuration, "Expected Configuration to be set correctly")
}

func TestMainFuncService(t *testing.T) {
	// Set up a mock configuration
	configuration := config.New("../.env")
	mainRepository := repository.NewMainRepository(&configuration)

	// Create the service
	service := NewMainService(&mainRepository, &configuration)

	// Call the method under test
	result := service.MainFuncService([]request.DataDetail{})

	// Assertions
	assert.Nil(t, result, "Expected MainFuncService to return nil")
}
