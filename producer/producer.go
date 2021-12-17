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
	"github.com/riferrei/srclient"
	"github.com/spf13/viper"
)

type TweetObject struct {
	User      string `json:"user"`
	Text      string `json:"text"`
	CreatedAt string `json:"createdAt"`
}

func main() {

	viper.SetConfigFile(".env")
	viper.ReadInConfig()

	bootstrapServer := viper.Get("BOOTSTRAP_SERVER")
	registryUrl := viper.Get("REGISTRY_URL").(string)

	consumerKey := viper.Get("TWITTER_CONSUMER_KEY").(string)
	consumerSecret := viper.Get("TWITTER_CONSUMER_SECRET").(string)

	accessToken := viper.Get("TWITTER_ACCESS_TOKEN").(string)
	accessSecret := viper.Get("TWITTER_ACCESS_SECRET").(string)

	clientID := viper.Get("SASL_USERNAME").(string)
	clientSecret := viper.Get("SASL_PASSWORD").(string)
	topicName := viper.Get("TOPIC").(string)

	schemaPath := viper.Get("SCHEMA_FILE_PATH").(string)

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(registryUrl)
	schemaRegistryClient.SetCredentials(clientID, clientSecret)

	schema, err := schemaRegistryClient.GetLatestSchema(topicName)
	if err != nil {
		fmt.Println(err.Error())
	}

	if schema == nil {
		schemaBytes, _ := ioutil.ReadFile(schemaPath)
		schema, err = schemaRegistryClient.CreateSchema(topicName, string(schemaBytes), srclient.Avro)
		if err != nil {
			panic(fmt.Sprintf("Error creating the schema %s", err))
		}
	}

	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServer,
		"acks":              "all",
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"sasl.username":     clientID,
		"sasl.password":     clientSecret,
	})

	config := oauth1.NewConfig(consumerKey, consumerSecret)
	token := oauth1.NewToken(accessToken, accessSecret)
	httpClient := config.Client(oauth1.NoContext, token)

	// Twitter client
	client := twitter.NewClient(httpClient)

	filterParams := &twitter.StreamFilterParams{
		StallWarnings: twitter.Bool(true),
		Track:         []string{"javascript"},
	}

	stream, err := client.Streams.Filter(filterParams)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("after stream")

	demux := twitter.NewSwitchDemux()

	fmt.Println("after demux")

	demux.Tweet = func(tweet *twitter.Tweet) {

		fmt.Println("tweet posted at", tweet.CreatedAt)

		tweetObj := TweetObject{
			User:      tweet.User.Name,
			Text:      tweet.Text,
			CreatedAt: tweet.CreatedAt,
		}

		b, err := json.Marshal(tweetObj)
		if err != nil {
			log.Fatal(err)
		}

		native, _, _ := schema.Codec().NativeFromTextual(b)
		valueBytes, _ := schema.Codec().BinaryFromNative(nil, native)

		var recordValue []byte
		recordValue = append(recordValue, valueBytes...)

		deliveryChan := make(chan kafka.Event)

		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topicName},
			Value:          recordValue,
		}, deliveryChan)

		if err != nil {
			log.Fatal(err)
		}
	}

	go demux.HandleChan(stream.Messages)

	// Wait for SIGINT and SIGTERM (HIT CTRL-C)
	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	log.Println(<-ch)

	fmt.Println("Stopping Stream...")
	stream.Stop()

}
