package main

import (
	"fmt"
	"html"
	"log"
	"net/http"

	"github.com/Shopify/sarama"
)

const topic = "sample-tioc"
// My name is gaurav
func main() {
	producer, err := newProducer()
	if err != nil {
		fmt.Println("Could not create producer: ", err)
	}
     fmt.Printf("hii")
	consumer, err := sarama.NewConsumer(brokers, nil)
	if err != nil {
		fmt.Println("Could not create consumer: ", err)
	}
	fmt.Printf("hii\n")
	subscribe(topic, consumer)
	fmt.Printf("hii\n")
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "Hello Sarama!") })
	fmt.Printf("hii\n")
	http.HandleFunc("/save", func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		r.ParseForm()
		msg := prepareMessage(topic, r.FormValue("q"))
		partition, offset, err := producer.SendMessage(msg)
		fmt.Fprintf(w, "Message was saved to partion: %d.\nMessage offset is: %d.\n %s error occured.", partition, offset, err.Error())
	})
	fmt.Printf("hii1")
	http.HandleFunc("/retrieve", func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, html.EscapeString(getMessage())) })
	fmt.Printf("hii\n")
	log.Fatal(http.ListenAndServe(":8081",nil))
	fmt.Printf("hii")}
var fakeDB string

func saveMessage(text string) { fakeDB = text }

func getMessage() string { return fakeDB }

var brokers = []string{"localhost:9092"}

func newProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	producer, err := sarama.NewSyncProducer(brokers, config)
	fmt.Printf("hii2")
	return producer, err
}

func prepareMessage(topic, message string) *sarama.ProducerMessage {
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Partition: -1,
		Value:     sarama.StringEncoder(message),
	}
	fmt.Printf("hii3")
	return msg
}

func subscribe(topic string, consumer sarama.Consumer) {
	partitionList, err := consumer.Partitions(topic) //get all partitions on the given topic
	if err != nil {
		fmt.Println("Error retrieving partitionList ", err)
	}
	initialOffset := sarama.OffsetOldest //get offset for the oldest message on the topic
	fmt.Printf("hii7")
	for _, partition := range partitionList {
		pc, _ := consumer.ConsumePartition(topic, partition, initialOffset)

		go func(pc sarama.PartitionConsumer) {
			for message := range pc.Messages() {
				messageReceived(message)
			}
		}(pc)
	}
}

func messageReceived(message *sarama.ConsumerMessage) {
	saveMessage(string(message.Value))
	fmt.Printf("hii4")
}