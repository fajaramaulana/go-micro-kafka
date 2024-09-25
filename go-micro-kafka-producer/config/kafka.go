package config

import (
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
)

func ConnectProducer(brokersUrl []string) (sarama.SyncProducer, error) {

	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5

	conn, err := sarama.NewSyncProducer(brokersUrl, config)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

// Retry logic for connecting to Kafka
func RetryKafkaConnection(brokersUrl []string, maxRetries int, retryInterval time.Duration) (producer sarama.SyncProducer, err error) {
	for i := 0; i < maxRetries; i++ {
		producer, err = ConnectProducer(brokersUrl)
		if err == nil {
			return producer, nil
		}
		log.Warn().Msgf("Kafka connection failed. Retry %d/%d", i+1, maxRetries)
		time.Sleep(retryInterval) // Wait before retrying
	}

	return nil, fmt.Errorf("failed to connect to Kafka after %d retries: %v", maxRetries, err)
}
