package service

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/config"
	kafkaconfig "github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/kafkaconfig"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/model/response"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/repository"
	"github.com/rs/zerolog/log"
)

type mainServiceImpl struct {
	Configuration config.Config
	Producer      kafkaconfig.KafkaProducer
	Repository    repository.MainRepository
}

func NewMainService(configuration *config.Config, producer kafkaconfig.KafkaProducer, repository repository.MainRepository) MainService {
	return &mainServiceImpl{
		Configuration: *configuration,
		Producer:      producer,
		Repository:    repository,
	}
}

func (s *mainServiceImpl) PublishQueueMain() {
	// Retrieve data from repository
	data, err := s.Repository.GetData()
	if err != nil {
		log.Error().Msg("Failed to get data")
		return
	}
	fmt.Printf("%# v\n", data)

	// Check if data is nil
	if data == nil {
		log.Error().Msg("No data retrieved from repository")
		return
	}

	// Prepare time and location data
	t := time.Now()
	location, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		log.Error().Msg("Failed to load location")
		return
	}
	log.Info().Msgf("Time: %s Total Data: %d", t.In(location), len(data.Data))

	if len(data.Data) > 0 {
		// Chunk data
		chunkSize, err := strconv.Atoi(s.Configuration.Get("SYSTEM_CHUNK_QUEUE"))
		if err != nil {
			chunkSize = 20
		}
		chunkedData := ChunkData(data.Data, chunkSize)

		// Loop through chunks and send messages
		for _, chunk := range chunkedData {
			mesData := response.GetData{
				Status:  true,
				Message: "Sukses",
				Time:    time.Now().String(),
				Data:    chunk,
			}

			message, err := json.Marshal(mesData)
			if err != nil {
				log.Error().Msg("Failed to marshal message.")
				continue
			}

			// Send message via Kafka
			err = s.SendMessageToKafka(s.Producer, message)
			if err != nil {
				log.Error().Msgf("Failed to send message: %v", err)
			}
		}
	} else {
		log.Info().Msg("Data is empty")
	}
}

// Private helper function to send message to Kafka
func (s *mainServiceImpl) SendMessageToKafka(producer kafkaconfig.KafkaProducer, message []byte) error {
	topic := s.Configuration.Get("KAFKA_TOPIC_MAIN")
	err := producer.SendMessage(topic, message)
	if err != nil {
		log.Error().Msgf("Failed to send message: %v", err)
	} else {
		log.Info().Msgf("Message sent to topic %s successfully", topic)
	}
	return err
}

func ChunkData(data []response.DataDetail, chunkSize int) [][]response.DataDetail {
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
