package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	cluster "github.com/bsm/sarama-cluster"
	"github.com/olivere/elastic/v7"
	"github.com/spf13/viper"
)

// KafkaElasticDoc is the elasic document object for each kafka message
type KafkaElasticDoc struct {
	TimestampNano  int64  `json:"timestamp-nano"`
	TimestampPST   string `json:"timestamp-pst"`
	IndexTimestamp int64  `json:"index-timestamp"`
	Partition      int32  `json:"partition"`
	Key            string `json:"key"`
	Value          string `json:"value"`
	Offset         int64  `json:"offset"`
	ValueRaw       string `json:"value-raw"`
	Topic          string `json:"topic"`
}

// KafkaElastic is the main application struct
type KafkaElastic struct {
	kafkaClient        sarama.Client
	kafkaBrokerList    []string
	elasticClient      *elastic.Client
	wg                 sync.WaitGroup
	consumerShutdowns  []chan bool
	ctx                context.Context
	cancel             context.CancelFunc
	indexQueue         chan *KafkaElasticDoc
	indexQueueShutdown chan bool
}

// NewKafkaElastic create a new kafka elastic instance
func NewKafkaElastic(brokers, elasticHosts []string) (*KafkaElastic, error) {
	ke := &KafkaElastic{}
	ke.kafkaBrokerList = brokers
	ke.indexQueue = make(chan *KafkaElasticDoc)
	ke.indexQueueShutdown = make(chan bool)
	ke.ctx, ke.cancel = context.WithCancel(context.Background())
	config := sarama.NewConfig()
	config.ClientID = "kafka-elastic"
	var err error
	ke.kafkaClient, err = sarama.NewClient(brokers, config)

	if err != nil {
		return ke, err
	}

	es, err := elastic.NewClient(elastic.SetErrorLog(logElastic), elastic.SetURL(elasticHosts...))

	if err != nil {
		return nil, err
	}

	ke.elasticClient = es

	return ke, nil
}

// GetTopics gets a list of all kakfa topics except internal topics and '__consumer_offsets' is explicitly ignored.
func (ke *KafkaElastic) GetTopics() ([]string, error) {
	topics, _ := ke.kafkaClient.Topics()

	return topics, nil
}

// RefreshTopicIndexes created indexes for new topics
func (ke *KafkaElastic) RefreshTopicIndexes() error {
	topics, err := ke.GetTopics()
	if err != nil {
		return err
	}

	for _, topic := range topics {
		exists, err := ke.elasticClient.IndexExists(topic).Do(context.Background())
		if err != nil {
			logElastic.Error(err)
			continue
		}

		if !exists {
			_, err := ke.elasticClient.CreateIndex(topic).Body(mapping).Do(context.Background())
			if err != nil {
				logElastic.Error(err)
				continue
			}
		}
	}

	return nil
}

func (ke *KafkaElastic) startConsumers() error {
	topics, err := ke.GetTopics()
	if err != nil {
		return err
	}

	for _, topic := range topics {
		err = ke.consumeTopic(topic)
		// Should we continue?
		if err != nil {
			return err
		}
	}

	return nil
}

func (ke *KafkaElastic) consumeTopic(topic string) error {
	group := "kafka_elastic_" + topic
	topics := []string{topic}

	consumerConfig := cluster.NewConfig()
	consumerConfig.Config.Consumer.MaxProcessingTime = 15 * time.Second
	consumerConfig.Config.Version = sarama.V0_10_0_0
	consumerConfig.Config.ClientID = group
	consumerConfig.Consumer.Offsets.Initial = sarama.OffsetOldest
	consumerConfig.Consumer.Return.Errors = true
	consumerConfig.Group.Return.Notifications = true

	// Build the consumer group
	consumer, consumerErr := cluster.NewConsumer(ke.kafkaBrokerList, group, topics, consumerConfig)
	if consumerErr != nil {
		return consumerErr
	}

	logger := logKafka.WithField("topic", topic)

	// consume errors
	ke.wg.Add(1)
	go func() {
		defer ke.wg.Done()
		for err := range consumer.Errors() {
			logger.Error(err.Error())
		}
	}()

	// consume notifications
	ke.wg.Add(1)
	shutdown := make(chan bool)
	ke.consumerShutdowns = append(ke.consumerShutdowns, shutdown)
	go func() {
		defer ke.wg.Done()
		for ntf := range consumer.Notifications() {
			logger.Info(fmt.Sprintf("Rebalanced: %+v\n", ntf))
		}
	}()

	// Consume messages
	ke.wg.Add(1)
	go func() {
		defer ke.wg.Done()
		logKafka.Info("Listening for messages on topic: " + topic)
		for {
			select {
			case kmessage, ok := <-consumer.Messages():
				if ok {
					doc := KafkaElasticDoc{
						Offset:         kmessage.Offset,
						Value:          "[replace]",
						TimestampNano:  kmessage.Timestamp.UnixNano(),
						TimestampPST:   kmessage.Timestamp.Format(time.RFC1123Z),
						IndexTimestamp: time.Now().UnixNano(),
						Partition:      kmessage.Partition,
						Key:            string(kmessage.Key),
						ValueRaw:       string(kmessage.Value),
						Topic:          topic,
					}
					ke.indexQueue <- &doc
					consumer.MarkOffset(kmessage, "")
				}
			case <-shutdown:
				logger.Info("Gracefully closing the " + topic + " consumer")
				consumer.Close()
				return

			}
		}
	}()
	return nil
}

func (ke *KafkaElastic) startIndexQueue() error {
	ke.wg.Add(1)

	go func() {
		logElastic.Info("Starting indexer...")
		bulkRequest := ke.elasticClient.Bulk()
		ticker := time.NewTicker(time.Second * 5)

		defer ke.wg.Done()
		for {
			select {
			case item := <-ke.indexQueue:
				jsonObj, err := json.Marshal(item)
				if err != nil {
					logElastic.Error(err)
					continue
				}
				value := item.ValueRaw

				// Number's are valid json, but need to be treated as raw value
				_, intErr := strconv.ParseInt(item.ValueRaw, 10, 64)
				if !json.Valid([]byte(item.ValueRaw)) || intErr == nil {
					value = `{"value-raw": "` + value + `"}`
				}

				jsonString := strings.Replace(string(jsonObj), "\"[replace]\"", value, 1)

				bulkRequest.Add(elastic.NewBulkIndexRequest().
					Index(item.Topic).
					Id(strconv.FormatInt(item.Offset, 10)).
					Doc(jsonString))
				if bulkRequest.NumberOfActions() > 1000 {
					// Do sends the bulk requests to Elasticsearch
					_, err := bulkRequest.Do(context.Background())
					if err != nil {
						logElastic.Error(err)
					} else {
						logElastic.Info("Sent 1000 messages to be index")
					}
				}
			case <-ticker.C:
				if bulkRequest.NumberOfActions() > 0 {
					// Do sends the bulk requests to Elasticsearch
					total := strconv.Itoa(bulkRequest.NumberOfActions())

					_, err := bulkRequest.Do(context.Background())
					if err != nil {
						logElastic.Error(err)
					} else {
						logElastic.Info("Sent " + total + " messages to be index")
					}
				}
			case <-ke.indexQueueShutdown:
				return
			}
		}
	}()

	return nil
}

// Start boot up the kafka elastic daemon process
func (ke *KafkaElastic) Start() error {
	err := ke.RefreshTopicIndexes()
	if err != nil {
		return err
	}

	ke.startIndexQueue()

	err = ke.startConsumers()

	if err != nil {
		return err
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	logMain.Info("Got interrupt signal. Initiating Shutdown.")

	ke.shutdown()

	return nil
}

func (ke *KafkaElastic) shutdown() error {
	logMain.Info("Shutting down kafka broker")
	if err := ke.kafkaClient.Close(); err != nil {
		return err
	}

	logMain.Info("Shutting down consumers")
	for _, shutdown := range ke.consumerShutdowns {
		shutdown <- true
	}

	logMain.Info("Shutting down indexer")
	ke.indexQueueShutdown <- true

	// Wait for the consumers to finish
	logMain.Info("Waiting for processes to finish")
	ke.wg.Wait()

	return nil
}

func main() {

	viper.SetConfigName("kafka-elastic")       // name of config file (without extension)
	viper.AddConfigPath("/etc/kafka-elastic/") // path to look for the config file in
	viper.AddConfigPath(".")
	viper.SetConfigType("yaml")
	err := viper.ReadInConfig() // Find and read the config file

	if err != nil { // Handle errors reading the config file
		panic(fmt.Errorf("fatal error config file: %s ", err))
	}

	kafkaElastic, err := NewKafkaElastic(viper.GetStringSlice("kafka-brokers"), viper.GetStringSlice("elastic-hosts"))

	if err != nil {
		logMain.Error("Failed to create kafka elastic")
		logMain.Fatalln(err)
	}

	err = kafkaElastic.Start()
	if err != nil {
		logMain.Fatal(err)
	}
}
