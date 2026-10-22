package cli

import (
	"errors"
	"fmt"

	"github.com/stellar-optics/stellar-prism/pkg/sink"
)

// Broker constructors are variables so tests can substitute in-memory fakes
// and exercise the wiring without a broker. Production code never reassigns
// them; only tests do, and they restore the originals afterwards.
var (
	newKafkaProducer = func(brokers []string) sink.KafkaProducer {
		return sink.NewKafkaProducer(brokers)
	}
	newAMQPPublisher = func(url string, opts sink.AMQPOptions) (sink.AMQPPublisher, error) {
		return sink.NewAMQPPublisher(url, opts)
	}
)

// validateBroker rejects partial or contradictory broker configuration before
// any connection is attempted, so a typo is a usage error rather than a dial
// timeout.
func (s *streamFlags) validateBroker() error {
	if s.kafkaTopic != "" && s.amqpQueue != "" {
		return fmt.Errorf("--kafka-topic and --amqp-queue select different sinks; configure only one")
	}
	if s.kafkaTopic != "" || len(s.kafkaBrokers) > 0 {
		if s.kafkaTopic == "" {
			return fmt.Errorf("--kafka-brokers requires --kafka-topic")
		}
		if len(s.kafkaBrokers) == 0 {
			return fmt.Errorf("--kafka-topic requires --kafka-brokers")
		}
		if _, err := sink.ParseKafkaPartition(s.kafkaPartitionBy); err != nil {
			return err
		}
	}
	if s.amqpURL != "" || s.amqpQueue != "" {
		if s.amqpURL == "" {
			return fmt.Errorf("--amqp-queue requires --amqp-url")
		}
		if s.amqpQueue == "" {
			return fmt.Errorf("--amqp-url requires --amqp-queue")
		}
	}
	return nil
}

// newKafkaSink builds the Kafka sink described by the flags.
func (s *streamFlags) newKafkaSink() (*sink.Kafka, error) {
	partitionBy, err := sink.ParseKafkaPartition(s.kafkaPartitionBy)
	if err != nil {
		return nil, err
	}

	producer := newKafkaProducer(s.kafkaBrokers)
	if producer == nil {
		return nil, fmt.Errorf("kafka: no producer was constructed")
	}

	k, err := sink.NewKafka(sink.KafkaConfig{
		Topic:       s.kafkaTopic,
		PartitionBy: partitionBy,
		Decode:      !s.raw,
	}, producer)
	if err != nil {
		return nil, errors.Join(err, producer.Close())
	}
	return k, nil
}

// newAMQPSink builds the AMQP sink described by the flags, declaring the
// durable queue as part of connecting.
func (s *streamFlags) newAMQPSink() (*sink.AMQP, error) {
	publisher, err := newAMQPPublisher(s.amqpURL, sink.AMQPOptions{Queue: s.amqpQueue})
	if err != nil {
		return nil, err
	}

	a, err := sink.NewAMQP(sink.AMQPConfig{
		Queue:      s.amqpQueue,
		Exchange:   s.amqpExchange,
		RoutingKey: s.amqpRoutingKey,
		Decode:     !s.raw,
	}, publisher)
	if err != nil {
		return nil, errors.Join(err, publisher.Close())
	}
	return a, nil
}
