
# Go Micro Kafka Consumer 🚀

Welcome to the **Go Micro Kafka Consumer**! This project is designed to complement your microservices architecture by providing a reliable Kafka consumer that processes messages efficiently. Built with Go and following best practices in software design, this consumer is your solution for seamless data ingestion.

## 📦 Features

- **Seamless Integration**: Easily integrate with existing Go applications and microservices.
- **Robust Kafka Support**: Leverage the power of Apache Kafka for scalable and high-throughput message consumption.
- **Flexible Configuration**: Support for configuration through `.env` files to manage different environments easily.
- **Comprehensive Error Handling**: Detailed error reporting for efficient debugging.
- **Testing Utilities**: Built-in testing capabilities to ensure the reliability of your message processing logic.


## 📜 Getting Started

### Prerequisites

- Go 1.18 or later
- Apache Kafka

### Installation

Clone the repository:

```bash
git clone https://github.com/fajaramaulana/go-micro-kafka.git
cd go-micro-kafka/go-micro-kafka-consumer
```
Install dependencies:
```bash
go mod tidy
```
### Configuration

Create a `.env` file in the root directory with the following content:
```dotenv
KAFKA_URL=localhost:9092
KAFKA_GROUP=your-consumer-group
KAFKA_TOPIC_MAIN=main_topic
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
    log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	configuration := config.New()

	// Set up Kafka connection
	producer, err := setupKafka(configuration)

	if err != nil {
		os.Exit(1)
	}
	// No need to close the producer as it does not have a Close method

	// Initialize the services and controller
	_, mainController := initializeServices(configuration, producer)

	// Start the cron job
	startCronJob(configuration, mainController)

	select {}
}
```

### Running Tests

To run the tests, execute:

bash

Copy code

```bash 
go test ./...
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
