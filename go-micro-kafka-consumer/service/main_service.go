package service

import (
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-consumer/model/request"
	"github.com/gofiber/fiber/v2"
)

type MainService interface {
	MainFuncService(params []request.DataDetail) *fiber.Map
}
