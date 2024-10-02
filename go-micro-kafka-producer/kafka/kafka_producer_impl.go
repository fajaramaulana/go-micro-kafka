package kafkaconfig

import (
	"github.com/IBM/sarama"
	"github.com/rs/zerolog/log"
)

// SaramaProducer is a wrapper around sarama.SyncProducer that implements the KafkaProducer interface
type SaramaProducer struct {
	producer sarama.SyncProducer
}

// NewSaramaProducer creates a new SaramaProducer
func NewSaramaProducer(producer sarama.SyncProducer) *SaramaProducer {
	return &SaramaProducer{producer: producer}
}

// SendMessage implements the KafkaProducer interface
func (p *SaramaProducer) SendMessage(topic string, message []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}

	partition, offset, err := p.producer.SendMessage(msg)
	if err != nil {
		log.Error().Msgf("Failed to send message to Kafka: %v", err)
		return err
	}

	log.Info().Msgf("Message sent to Kafka topic(%s)/partition(%d)/offset(%d)", topic, partition, offset)
	return nil
}
