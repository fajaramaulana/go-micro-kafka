package repository

import (
	"strconv"

	"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/model/response"
	"github.com/google/uuid"
)

func NewMainRepository() MainRepository {
	return &mainRepositoryImpl{}
}

type mainRepositoryImpl struct {
}

func (m *mainRepositoryImpl) GetData() (*response.GetData, error) {
	// data detail

	count := 5
	dataDetail := []response.DataDetail{}
	for i := 0; i < count; i++ {
		dataDetail = append(dataDetail, response.DataDetail{
			Uuid: uuid.New().String(),
			Name: "Name " + strconv.Itoa(i),
			Age:  20,
		})
	}

	retData := response.GetData{
		Message: "Hello World",
		Status:  true,
		Time:    "2024-09-24",
		Data:    dataDetail,
	}

	return &retData, nil

}
