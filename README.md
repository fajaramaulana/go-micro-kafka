
# Go Micro Kafka 🌐

  

Welcome to **Go Micro Kafka**! This repository is a collection of microservices built with Go and powered by Apache Kafka, designed to provide efficient, scalable, and reliable messaging solutions for modern applications.

## 🚀 Overview

Go Micro Kafka is structured to facilitate the development and deployment of microservices that communicate through Kafka. With a focus on clean architecture and best practices, this project empowers developers to create robust distributed systems.

## 📦 Features

-  **Microservice Architecture**: Easily create and manage multiple microservices.

-  **Kafka Integration**: Seamless integration with Apache Kafka for high-performance messaging.

-  **Extensible Design**: Modular components that can be easily extended or replaced.

-  **Testing and Mocking**: Comprehensive test coverage with mocking capabilities for unit tests.

-  **Configuration Management**: Environment-based configuration management using `.env` files.

## 📜 Getting Started

### Prerequisites

- Go 1.18 or later

- Apache Kafka

### Installation Clone the repository:

```bash

git  clone  https://github.com/fajaramaulana/go-micro-kafka.git  cd  go-micro-kafka

```

### Project Structure
```
├── go-micro-kafka-producer/

│ └── config

│ │ ├── config.go

│ │ └── kafka.go

│ └── exception

│ │ ├── data_not_found_error.go

│ │ ├── database_error.go

│ │ ├── error_handler.go

│ │ ├── error.go

│ │ └── general_error.go

│ └── mocks

│ │ ├── mock_consumer_group_claim.go

│ │ ├── mock_consumer_group_session.go

│ │ └── mock_main_service.go

│ └── model

│ │ ├── request

│ │ │ └── main_request.go

│ │ └── response

│ │ │ ├── main_response.go

│ │ │ └── web_response.go

│ └── repository

│ │ ├── main_repository_impl.go

│ │ ├── main_repository_test.go

│ │ └── main_repository.go

│ └── service

│ │ ├── main_service_impl.go

│ │ ├── main_service_test.go

│ │ └── main_service.go

│ ├── go.mod

│ ├── go.sum

│ ├── main.go

│ ├── README.md

│ └── .env

├── go-micro-kafka-consumer/

│ └── config

│ │ ├── config.go

│ │ └── kafka.go

│ └── controller

│ │ ├── main_controller_test.go

│ │ └── main_controller.go

│ └── exception

│ │ ├── data_not_found_error.go

│ │ ├── database_error.go

│ │ ├── error_handler.go

│ │ ├── error.go

│ │ └── general_error.go

│ └── kafkaconfig

│ │ ├── kafka_producer_impl_test.go

│ │ ├── kafka_producer_impl.go

│ │ └── kafka_producer.go

│ └── mock

│ │ ├── mock_kafka_producer.go

│ │ ├── mock_main_repository.go

│ │ ├── mock_main_service.go

│ │ └── mock_test.go

│ └── model

│ │ └── response

│ │ │ ├── main_response.go

│ │ │ └── web_response.go

│ └── repository

│ │ ├── main_repository_impl.go

│ │ ├── main_repository_test.go

│ │ └── main_repository.go

│ └── service

│ │ ├── main_service_impl.go

│ │ ├── main_service_test.go

│ │ └── main_service.go

│ ├── go.mod

│ ├── go.sum

│ ├── main.go

│ ├── README.md

│ └── .env

└── kafka

└── docker-compose.yml
```
  

### Configuration

  

Create a `.env` file in the root directory with the following content:

```dotenv

KAFKA_BROKER=localhost:9092

```

### Usage

  

To start sending messages to Kafka, use the following code snippet:

go

  

Copy code

  

```go

package  main

  

import (

"github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/kafkaconfig"

)

  

func  main() {

producer := kafkaconfig.NewKafkaProducer()

err := producer.SendMessage("main_topic", []byte("Hello Kafka!"))

if  err != nil {

panic(err)

}

}

```

  

### Running Tests

  

To run the tests, execute:

  

bash

  

Copy code

  

```bash

go  test  ./...

```

## 🔍 Contributing

  

We welcome contributions! Please fork the repository and submit a pull request. Make sure to follow the coding standards and include tests for new features.

  

## 🤝 License

  

This project is licensed under the MIT License. See the LICENSE file for details.

  

## 🌟 Acknowledgments

  

- Thanks to the [Apache Kafka](https://kafka.apache.org/) community for their incredible work on the messaging system.

- Special thanks to the contributors for their efforts in enhancing this project.

<hr>

Unleash the power of microservices with Go Micro Kafka Producer! For questions or feedback, feel free to [open an issue](https://github.com/fajaramaulana/go-micro-kafka/issues)
