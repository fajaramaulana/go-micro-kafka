package service

import (
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/config"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/model/request"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/repository"
	"github.com/gofiber/fiber/v2"
)

type mainServiceImpl struct {
	MainRepository repository.MainRepository
	Configuration  config.Config
}

func NewMainService(mainRepository *repository.MainRepository, configuration *config.Config) MainService {
	return &mainServiceImpl{
		MainRepository: *mainRepository,
		Configuration:  *configuration,
	}
}

func (m *mainServiceImpl) MainFuncService(params []request.DataDetail) *fiber.Map {
	return nil
}
