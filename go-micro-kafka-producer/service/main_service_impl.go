package service

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/IBM/sarama"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/config"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/model/response"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/repository"
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
		fmt.Println(err)
	}

	t := time.Now()
	location, err := time.LoadLocation("Asia/Jakarta")
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("Time: ", t.In(location), "Total Data: ", len(data.Data))
	if len(data.Data) > 0 {
		chunkSize, err := strconv.Atoi(s.Configuration.Get("SYSTEM_CHUNK_QUEUE"))
		if err != nil {
			chunkSize = 20
		}

		chunkedData := chunkData(data.Data, chunkSize)
		for _, chunk := range chunkedData {
			message, err := json.Marshal(chunk)
			if err != nil {
				fmt.Println(err)
			}

			brokersUrl := []string{s.Configuration.Get("KAFKA_URL")}
			producer, err := config.ConnectProducer(brokersUrl)
			if err != nil {
				fmt.Println(err)
			}

			defer producer.Close()

			msg := &sarama.ProducerMessage{
				Topic: s.Configuration.Get("KAFKA_TOPIC"),
				Value: sarama.StringEncoder(message),
			}

			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				fmt.Println(err)
			}

			fmt.Printf("Message size %d/%d is stored in topic(%s)/partition(%d)/offset(%d)\n", len(chunk), len(data.Data), s.Configuration.Get("KAFKA_TOPIC"), partition, offset)
		}
	} else {
		fmt.Println("Data is empty")
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
