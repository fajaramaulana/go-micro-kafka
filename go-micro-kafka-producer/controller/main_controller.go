package controller

import "github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/service"

type MainController struct {
	MainService service.MainService
}

func NewMainController(mainService *service.MainService) MainController {
	return MainController{MainService: *mainService}
}

func (controller *MainController) PublishMessageMain() {
	controller.MainService.PublishQueueMain()
}
