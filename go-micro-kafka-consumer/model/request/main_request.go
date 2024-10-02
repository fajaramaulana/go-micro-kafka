package request

type ParamsId struct {
	ID string `json:"id"`
}

type DataDetail struct {
	Uuid string `json:"uuid"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}

type DataFromKafka struct {
	Status  bool         `json:"status"`
	Message string       `json:"message"`
	Data    []DataDetail `json:"data"`
}
