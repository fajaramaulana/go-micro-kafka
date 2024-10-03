package controller

import (
	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/service"
)

type MainController struct {
	Service service.MainService
}

// NewMainController creates a new MainController
func NewMainController(service service.MainService) *MainController {
	return &MainController{Service: service}
}

// PublishMessageMain publishes the main message to Kafka
func (controller *MainController) PublishMessageMain() {
	controller.Service.PublishQueueMain()
}
