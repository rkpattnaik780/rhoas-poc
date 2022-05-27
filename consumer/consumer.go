package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
	"github.com/riferrei/srclient"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func main() {

	// load .env files that hold configurations MongoDB, Twitter and Managed Services
	err := godotenv.Load("../.env", "../rhoas.env")
	if err != nil {
		log.Fatal(err.Error())
	}

	// Configuring a client to connect to the MongoDB instance
	mongoURI := os.Getenv("MONGO_URI")
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()
	serverAPIOptions := options.ServerAPI(options.ServerAPIVersion1)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(mongoURI).SetServerAPIOptions(serverAPIOptions))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	// Execute a ping command to verify that the client can connect to the deployment
	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		log.Fatal(err)
	}

	// Parsing the environment variables from the .env files
	bootstrapServer := os.Getenv("KAFKA_HOST")
	registryUrl := os.Getenv("SERVICE_REGISTRY_URL")
	compatPath := os.Getenv("SERVICE_REGISTRY_COMPAT_PATH")

	registryAPIEndPoint := fmt.Sprintf("%s%s", registryUrl, compatPath)

	topicName := os.Getenv("TOPIC")

	clientID := os.Getenv("RHOAS_CLIENT_ID")

	clientSecret := os.Getenv("RHOAS_CLIENT_SECRET")

	dbName := os.Getenv("MONGO_DATABASE")
	dbCollection := os.Getenv("MONGO_COLLECTION")

	// Initialize a Kafka consumer to read messages from a Kafka topic
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

	// Subscribe to the topic
	err = consumer.SubscribeTopics([]string{topicName}, nil)
	if err != nil {
		log.Fatal(err)
	}

	collection := client.Database(dbName).Collection(dbCollection)

	// Creating a new client to interact with the Service Registry instance and fetch schema/artifact
	schemaRegistryClient := srclient.CreateSchemaRegistryClient(registryAPIEndPoint)
	schemaRegistryClient.SetCredentials(clientID, clientSecret)

	schema, err := schemaRegistryClient.GetLatestSchema(topicName)
	if err != nil {
		fmt.Println(err.Error())
	}

	for {

		// Poll the consumer for messages
		message, err := consumer.ReadMessage(1000 * time.Millisecond)
		if err != nil {
			fmt.Println(err.Error())
		}
		if err == nil {
			var tweetJSON map[string]interface{}

			// convert data in bytes to Go native data type
			// in accordance with the Avro schema supplied
			native, _, _ := schema.Codec().NativeFromBinary(message.Value)

			// Converts Go native data type to Avro in JSON text format
			// in accordance with the Avro schema supplied
			value, _ := schema.Codec().TextualFromNative(nil, native)
			json.Unmarshal(value, &tweetJSON)

			tweetRecord := bson.M{
				"user":      tweetJSON["user"],
				"text":      tweetJSON["text"],
				"createdAt": tweetJSON["createdAt"],
			}

			// insert tweet into the collection.
			res, err := collection.InsertOne(ctx, tweetRecord)
			if err != nil {
				log.Fatal(err)
			}

			// Log the ID of the object created for the tweet
			id := res.InsertedID
			fmt.Println("tweet inserted with ID: ", id)
		}
	}

}
