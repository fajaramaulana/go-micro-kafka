package service

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/config"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/model/response"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/repository"
	"github.com/rs/zerolog/log"
)

func NewMainService(configuration *config.Config, mainRepository *repository.MainRepository) MainService {
	return &mainServiceImpl{
		Configuration:  *configuration,
		MainRepository: *mainRepository,
	}
}

type mainServiceImpl struct {
	Configuration  config.Config
	MainRepository repository.MainRepository
}

func (s *mainServiceImpl) PublishQueueMain() {
	// get data from repository
	data, err := s.MainRepository.GetData()
	if err != nil {
		log.Error().Msg("Failed to get data")
	}

	t := time.Now()
	location, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		log.Error().Msg("Failed to load location")
	}
	log.Info().Msgf("Time: %s Total Data: %d", t.In(location), len(data.Data))
	if len(data.Data) > 0 {
		chunkSize, err := strconv.Atoi(s.Configuration.Get("SYSTEM_CHUNK_QUEUE"))
		if err != nil {
			chunkSize = 20
		}

		chunkedData := chunkData(data.Data, chunkSize)
		for _, chunk := range chunkedData {
			message, err := json.Marshal(chunk)
			if err != nil {
				log.Error().Msg("Failed to connect to Kafka.")
			}

			brokersUrl := []string{s.Configuration.Get("KAFKA_URL")}
			producer, err := config.RetryKafkaConnection(brokersUrl, 3, 1*time.Minute)
			if err != nil {
				log.Error().Msg("Failed to connect to Kafka after retries.")
				return
			}

			// Ensure the producer is closed only if it was successfully created
			defer producer.Close()

			msg := &sarama.ProducerMessage{
				Topic: s.Configuration.Get("KAFKA_TOPIC"),
				Value: sarama.StringEncoder(message),
			}

			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				log.Error().Msgf("Failed to send message: %v", err)
			}

			log.Info().Msgf("Message size %d/%d is stored in topic(%s)/partition(%d)/offset(%d)", len(chunk), len(data.Data), s.Configuration.Get("KAFKA_TOPIC"), partition, offset)
		}
	} else {
		log.Info().Msg("Data is empty")
	}
}

func chunkData(data []response.DataDetail, chunkSize int) [][]response.DataDetail {
	var chunks [][]response.DataDetail

	numChunks := (len(data) + chunkSize - 1) / chunkSize
	for i := 0; i < numChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(data) {
			end = len(data)
		}
		chunks = append(chunks, data[start:end])
	}

	return chunks
}
