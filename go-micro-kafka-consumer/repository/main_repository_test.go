package repository

import (
	"testing"

	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/config"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/model/request"
	"github.com/stretchr/testify/assert"
)

func TestMainFuncRepository(t *testing.T) {
	// Set up a mock configuration
	configuration := config.New("../.env") // Assuming New() initializes a config object
	repo := NewMainRepository(&configuration)

	// Create a test parameter
	params := &request.ParamsId{ID: "original_id"}

	// Call the method under test
	result, err := repo.MainFuncRepository(params)

	// Assertions
	assert.NoError(t, err, "Expected no error from MainFuncRepository")
	assert.Equal(t, "123", result, "Expected the returned ID to be '123'")
	assert.Equal(t, "123", params.ID, "Expected the params ID to be modified to '123'")
}
