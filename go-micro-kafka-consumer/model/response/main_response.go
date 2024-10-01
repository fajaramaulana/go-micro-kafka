package response

type GetData struct {
	Status  bool         `json:"status"`
	Message string       `json:"message"`
	Time    string       `json:"time"`
	Data    []DataDetail `json:"data"`
}

type DataDetail struct {
	Uuid string `json:"uuid"`
	Name string `json:"name"`
	Age  int    `json:"age"`
}
