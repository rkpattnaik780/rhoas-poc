package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/dghubble/go-twitter/twitter"
	"github.com/dghubble/oauth1"
	"github.com/joho/godotenv"
	"github.com/riferrei/srclient"
)

// TweetObject represents object that is to be created from a tweet
type TweetObject struct {
	User      string `json:"user"`
	Text      string `json:"text"`
	CreatedAt string `json:"createdAt"`
}

func main() {

	// load .env files that hold configurations MongoDB, Twitter and Managed Services
	err := godotenv.Load("../.env", "../rhoas.env")
	if err != nil {
		log.Fatal(err.Error())
	}

	// Parsing the environment variables from the .env files
	bootstrapServer := os.Getenv("KAFKA_HOST")

	registryUrl := os.Getenv("SERVICE_REGISTRY_URL")
	compatPath := os.Getenv("SERVICE_REGISTRY_COMPAT_PATH")

	consumerKey := os.Getenv("TWITTER_CONSUMER_KEY")
	consumerSecret := os.Getenv("TWITTER_CONSUMER_SECRET")

	accessToken := os.Getenv("TWITTER_ACCESS_TOKEN")
	accessSecret := os.Getenv("TWITTER_ACCESS_SECRET")

	clientID := os.Getenv("RHOAS_CLIENT_ID")
	clientSecret := os.Getenv("RHOAS_CLIENT_SECRET")
	topicName := os.Getenv("TOPIC")

	schemaPath := os.Getenv("SCHEMA_FILE_PATH")

	registryAPIEndPoint := fmt.Sprintf("%s%s", registryUrl, compatPath)

	// Creating a new client to interact with the Service Registry instance and fetch schema/artifact
	schemaRegistryClient := srclient.CreateSchemaRegistryClient(registryAPIEndPoint)
	schemaRegistryClient.SetCredentials(clientID, clientSecret)

	schema, err := schemaRegistryClient.GetLatestSchema(topicName)

	if err != nil {
		fmt.Println(err.Error())
	}

	// if schema doesn't exist, create one using the local avsc file
	if schema == nil {
		schemaBytes, _ := ioutil.ReadFile(schemaPath)
		schema, err = schemaRegistryClient.CreateSchema(topicName, string(schemaBytes), srclient.Avro)
		if err != nil {
			panic(fmt.Sprintf("Error creating the schema: %s", err))
		}
	}

	// Initialize a Kafka producer to write messages to Kafka topic
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServer,
		"acks":              "all",
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"sasl.username":     clientID,
		"sasl.password":     clientSecret,
	})

	if err != nil {
		log.Fatal(err)
	}

	// Initialize the twitter API client
	config := oauth1.NewConfig(consumerKey, consumerSecret)
	token := oauth1.NewToken(accessToken, accessSecret)
	httpClient := config.Client(oauth1.NoContext, token)

	// Twitter client
	client := twitter.NewClient(httpClient)

	// filter object to fetch specific tweets
	filterParams := &twitter.StreamFilterParams{
		StallWarnings: twitter.Bool(true),
		// The value of Track points to strings that the client should look up in tweets
		Track: []string{"javascript"},
	}

	stream, err := client.Streams.Filter(filterParams)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("after stream")

	// Returns a struct that holds do nothing handler functions
	demux := twitter.NewSwitchDemux()

	// Define handler function to be executed when a tweet is received
	demux.Tweet = func(tweet *twitter.Tweet) {

		tweetObj := TweetObject{
			User:      tweet.User.Name,
			Text:      tweet.Text,
			CreatedAt: tweet.CreatedAt,
		}

		b, err := json.Marshal(tweetObj)
		if err != nil {
			log.Fatal(err)
		}

		// convert data in JSON text format to Go native data types
		// in accordance with the Avro schema supplied
		native, _, _ := schema.Codec().NativeFromTextual(b)

		// appends the binary encoded representation of the native value
		// to bytes in accordance with the Avro schema supplied
		valueBytes, _ := schema.Codec().BinaryFromNative(nil, native)

		// var recordValue []byte
		// recordValue = append(recordValue, valueBytes...)

		deliveryChan := make(chan kafka.Event)

		// Send the message to Kafka
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topicName},
			Value:          valueBytes,
		}, deliveryChan)

		if err != nil {
			log.Fatal(err)
		}
	}

	go demux.HandleChan(stream.Messages)

	// Wait for SIGINT and SIGTERM (HIT CTRL-C)
	// To stop the producer component
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)

	fmt.Println("Stopping Stream...")
	stream.Stop()

}
