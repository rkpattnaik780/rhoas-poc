package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/riferrei/srclient"
	"github.com/spf13/viper"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {

	viper.SetConfigFile(".env")
	viper.ReadInConfig()

	mongoURI := viper.Get("MONGO_URI").(string)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI))
	if err != nil {
		log.Fatal(err)
	}

	defer client.Disconnect(ctx)
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatal(err)
	}
	databases, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("databases", databases)

	bootstrapServer := viper.Get("BOOTSTRAP_SERVER")
	registryUrl := viper.Get("REGISTRY_URL").(string)

	topicName := viper.Get("TOPIC").(string)

	clientID := viper.Get("SASL_USERNAME").(string)
	clientSecret := viper.Get("SASL_PASSWORD").(string)

	dbName := viper.Get("MONGO_DATABASE").(string)
	dbCollection := viper.Get("MONGO_COLLECTION").(string)

	consumer, newErr := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServer,
		"group.id":          "poc-group-2",
		"auto.offset.reset": "earliest",
		"security.protocol": "SASL_SSL",
		"sasl.mechanisms":   "PLAIN",
		"sasl.username":     clientID,
		"sasl.password":     clientSecret,
	})

	if newErr != nil {
		fmt.Printf("Failed to create consumer: %s\n", newErr)
		os.Exit(1)
	}

	err = consumer.SubscribeTopics([]string{topicName}, nil)
	if err != nil {
		log.Fatal(err)
	}

	collection := client.Database(dbName).Collection(dbCollection)

	fmt.Println("after collection statement")

	schemaRegistryClient := srclient.CreateSchemaRegistryClient(registryUrl)
	schemaRegistryClient.SetCredentials(clientID, clientSecret)

	schema, err := schemaRegistryClient.GetLatestSchema(topicName)
	if err != nil {
		fmt.Println(err.Error())
	}

	for {
		message, err := consumer.ReadMessage(1000 * time.Millisecond)
		if err != nil {
			fmt.Println(err.Error())
		}
		if err == nil {

			fmt.Println("message value - ", string(message.Value))
			var tweetJSON map[string]interface{}

			native, _, _ := schema.Codec().NativeFromBinary(message.Value)
			value, _ := schema.Codec().TextualFromNative(nil, native)
			fmt.Printf("Here is the message %s\n", string(value))
			json.Unmarshal(value, &tweetJSON)
			fmt.Println("tweet json:", tweetJSON)

			tweetRecord := bson.M{
				"user":      tweetJSON["user"],
				"text":      tweetJSON["text"],
				"createdAt": tweetJSON["createdAt"],
			}

			res, err := collection.InsertOne(ctx, tweetRecord)
			if err != nil {
				log.Fatal(err)
			}
			id := res.InsertedID
			fmt.Println(id)
		}
	}

}
