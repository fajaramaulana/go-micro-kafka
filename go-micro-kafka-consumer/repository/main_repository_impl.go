package repository

import (
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/config"
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/model/request"
	"github.com/rs/zerolog/log"
)

type mainRepositoryImpl struct {
	Configuration config.Config
}

func NewMainRepository(configuration *config.Config) MainRepository {
	return &mainRepositoryImpl{
		Configuration: *configuration,
	}
}

func (m *mainRepositoryImpl) MainFuncRepository(params *request.ParamsId) (string, error) {
	log.Info().Msg("MainFuncRepository")
	log.Info().Msg("Params ID Before: " + params.ID)
	params.ID = "123"
	log.Info().Msg("Params ID After: " + params.ID)
	return params.ID, nil
}
