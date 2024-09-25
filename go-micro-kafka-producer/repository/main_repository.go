package repository

import "github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/model/response"

type MainRepository interface {
	GetData() (*response.GetData, error)
}
