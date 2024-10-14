package config

import (
	"os"

	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/exception"
	"github.com/joho/godotenv"
)

type Config interface {
	Get(key string) string
	Set(key, value string)
}

type configImpl struct {
}

func (config *configImpl) Get(key string) string {
	return os.Getenv(key)
}

func (config *configImpl) Set(key, value string) {
	os.Setenv(key, value)
}

func New(filenames ...string) Config {
	err := godotenv.Load(filenames...)
	exception.PanicIfNeeded(err)
	return &configImpl{}
}
