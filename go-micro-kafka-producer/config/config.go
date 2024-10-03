package config

import (
	"os"

	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/exception"
	"github.com/joho/godotenv"
	"github.com/stretchr/testify/mock"
)

type Config interface {
	Get(key string) string
}

type configImpl struct {
}

func (config *configImpl) Get(key string) string {
	return os.Getenv(key)
}

func New(filenames ...string) Config {
	err := godotenv.Load(filenames...)
	exception.PanicIfNeeded(err)
	return &configImpl{}
}

// MockConfig is a mock version of your Config interface/struct
type MockConfig struct {
	mock.Mock
}

// Get mocks the Get method to return a specific value for a given key
func (m *MockConfig) Get(key string) string {
	args := m.Called(key)
	return args.String(0)
}
