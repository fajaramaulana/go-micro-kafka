package repository

import "github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/model/request"

type MainRepository interface {
	MainFuncRepository(params *request.ParamsId) (string, error)
}
