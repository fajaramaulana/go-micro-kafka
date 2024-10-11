
# Go Micro Kafka Producer 🚀

Welcome to the **Go Micro Kafka Producer**! This project is designed to streamline your microservices architecture using Kafka as a messaging system. Built with Go and utilizing best practices in software design, this producer is your gateway to efficient and reliable data processing.

## 📦 Features

- **Simple Integration**: Easily integrate with existing Go applications and microservices.
- **Robust Kafka Support**: Leverage the power of Apache Kafka for scalable and high-throughput messaging.
- **Mocking for Tests**: Built-in mocking capabilities to streamline testing and ensure reliability.
- **Environment Configurations**: Support for configuration through `.env` files, making it easy to manage different environments.
- **Comprehensive Error Handling**: Detailed error reporting to help debug issues efficiently.

## 📜 Getting Started

### Prerequisites

- Go 1.18 or later
- Apache Kafka

### Installation

Clone the repository:

```bash
git clone https://github.com/fajaramaulana/go-micro-kafka.git
cd go-micro-kafka/go-micro-kafka-producer
```
Install dependencies:
```bash
go mod tidy
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
package main

import (
    "github.com/fajaramaulana/go-micro-kafka/go-micro-kafka-producer/kafkaconfig"
)

func main() {
    producer := kafkaconfig.NewKafkaProducer()
    err := producer.SendMessage("main_topic", []byte("Hello Kafka!"))
    if err != nil {
        panic(err)
    }
}
```

### Running Tests

To run the tests, execute:

bash

Copy code

```bash 
go test ./...` 
```
## 🔍 Contributing

We welcome contributions! Please fork the repository and submit a pull request. Make sure to follow the coding standards and include tests for new features.

## 🤝 License

This project is licensed under the MIT License. See the LICENSE file for details.

## 🌟 Acknowledgments

-   Thanks to the [Apache Kafka](https://kafka.apache.org/) community for their incredible work on the messaging system.
-   Special thanks to the contributors for their efforts in enhancing this project.
<hr>
Unleash the power of microservices with Go Micro Kafka Producer! For questions or feedback, feel free to [open an issue](https://github.com/fajaramaulana/go-micro-kafka/issues)
