package service

import (
	"fmt"

	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/config"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/model/request"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/repository"
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog/log"
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
	// todo implement the business logic here
	fmt.Println("MainFuncService")

	// loop params
	for _, param := range params {
		// call repository
		paramsId := request.ParamsId{ID: param.Uuid}
		result, err := m.MainRepository.MainFuncRepository(&paramsId)

		if err != nil {
			log.Error().Msg("Error MainFuncRepository, " + err.Error())
			continue
		}

		fmt.Println("Result: ", result)
	}

	return nil
}
