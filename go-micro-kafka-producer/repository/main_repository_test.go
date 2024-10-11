package repository_test

import (
	"testing"

	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/repository"
	"github.com/stretchr/testify/assert"
)

func TestGetData(t *testing.T) {
	repo := repository.NewMainRepository()

	// Call the GetData method
	data, err := repo.GetData()

	// Check for any errors
	assert.NoError(t, err)

	// Check the returned data
	assert.NotNil(t, data)
	assert.Equal(t, "Hello World", data.Message)
	assert.True(t, data.Status)
	assert.Len(t, data.Data, 5) // Verify that there are 5 data details

	// Optionally, check if each data detail is valid
	for _, detail := range data.Data {
		assert.NotEmpty(t, detail.Uuid)
		assert.NotEmpty(t, detail.Name)
		assert.Equal(t, 20, detail.Age)
	}
}
