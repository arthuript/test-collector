package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"net/http"
	"os/signal"
	"syscall"
	"time"
	"strings"

	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var collection *mongo.Collection

// ResourceData é a estrutura do JSON que será recebida e armazenada
type ResourceData struct {
	SensorID string                 `json:"uuid"`
	Data     map[string]interface{} `json:"data"`
}


// Handle POST /resources/data - Get historical data of all resources
func handleGetAllResourcesData(mongoCollection *mongo.Collection) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Fetch all documents from MongoDB
		cursor, err := mongoCollection.Find(context.TODO(), bson.M{})
		if err != nil {
			http.Error(w, "Failed to fetch data from MongoDB", http.StatusInternalServerError)
			log.Printf("MongoDB find error: %v", err)
			return
		}
		defer cursor.Close(context.TODO())

		// Collect the results into a slice
		var results []bson.M
		if err := cursor.All(context.TODO(), &results); err != nil {
			http.Error(w, "Error processing MongoDB data", http.StatusInternalServerError)
			log.Printf("Cursor iteration error: %v", err)
			return
		}

		// Serialize the results to JSON
		response, err := json.Marshal(results)
		if err != nil {
			http.Error(w, "Error serializing response", http.StatusInternalServerError)
			log.Printf("JSON serialization error: %v", err)
			return
		}

		// Send the JSON to the client
		w.Write(response)
	}
}

// Handle POST /resources/{uuid}/data - Get historical data of a specific resource
func handleGetResourceData(mongoCollection *mongo.Collection) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Extract the UUID from the URL
		parts := strings.Split(r.URL.Path, "/")
		if len(parts) < 3 {
			http.Error(w, "Invalid request URL", http.StatusBadRequest)
			return
		}
		uuid := parts[2]

		// Query MongoDB for documents matching the UUID
		filter := bson.M{"uuid": uuid}
		cursor, err := mongoCollection.Find(context.TODO(), filter)
		if err != nil {
			http.Error(w, "Error fetching data from MongoDB", http.StatusInternalServerError)
			log.Printf("MongoDB find error: %v", err)
			return
		}
		defer cursor.Close(context.TODO())

		// Collect the results into a slice
		var results []ResourceData
		if err := cursor.All(context.TODO(), &results); err != nil {
			http.Error(w, "Error processing MongoDB data", http.StatusInternalServerError)
			log.Printf("Cursor iteration error: %v", err)
			return
		}

		// Serialize the results to JSON
		response, err := json.Marshal(results)
		if err != nil {
			http.Error(w, "Error serializing response", http.StatusInternalServerError)
			log.Printf("JSON serialization error: %v", err)
			return
		}
		log.Printf("JSON: %s", response)

		// Send the JSON to the client
		w.Write(response)
	}
}

func main() {
	// Configura o cliente MongoDB
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://mongo:27017"))
	if err != nil {
		log.Fatalf("Erro ao criar cliente MongoDB: %v", err)
	}
	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatalf("Erro ao conectar ao MongoDB: %v", err)
	}
	defer client.Disconnect(ctx)

	// Verifica se a conexão está funcionando
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatalf("Erro ao verificar conexão com MongoDB: %v", err)
	}
	log.Println("Conectado ao MongoDB com sucesso")
	collection = client.Database("intercity").Collection("sensors")

	// Conecta ao RabbitMQ
	rabbitMQURL := "amqp://guest:guest@rabbitmq:5672/"
	conn, err := amqp.Dial(rabbitMQURL)
	for err != nil {
		log.Println("Erro ao conectar ao RabbitMQ, tentando novamente daqui 2 segundos: %s", err)
		time.Sleep(2000 * time.Millisecond)
		conn, err = amqp.Dial(rabbitMQURL)
	}
	defer conn.Close()
	log.Println("Conectado ao RabbitMQ com sucesso")

	// Cria um canal no RabbitMQ
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Erro ao abrir um canal no RabbitMQ: %v", err)
	}
	defer ch.Close()
	
	// Declaração do exchange onde o tópico será publicado
	err = ch.ExchangeDeclare(
		"data_stream", // nome do exchange
		"topic",       // tipo de exchange
		true,          // durável
		false,         // auto-delete
		false,         // interno
		false,         // exclusivo
		nil,           // argumentos
	)
	if err != nil {
		log.Fatalf("Erro ao declarar exchange: %s", err)
	}

	// Declara a fila de mensagens (tópico)
	queueName := ""
	queue, err := ch.QueueDeclare(
		queueName, // nome da fila
		true,      // durável
		false,     // auto-delete
		false,     // exclusivo
		false,     // no-wait
		nil,       // argumentos
	)
	if err != nil {
		log.Fatalf("Erro ao declarar a fila no RabbitMQ: %v", err)
	}
	// Fazer o binding da fila ao exchange com uma routing key
	routingKey := "topic" // Deve coincidir com a chave usada pelo produtor
	err = ch.QueueBind(
		queue.Name,    // nome da fila
		routingKey,    // routing key
		"data_stream", // nome do exchange
		false,         // no-wait
		nil,           // argumentos adicionais
	)
	if err != nil {
	    log.Fatalf("Erro ao fazer o binding da fila: %v", err)
	}


	// Inscreve-se no tópico e consome mensagens
	msgs, err := ch.Consume(
		queueName, // nome da fila
		"",        // consumer
		true,      // auto-ack
		false,     // exclusivo
		false,     // no-local
		false,     // no-wait
		nil,       // argumentos
	)
	if err != nil {
		log.Fatalf("Erro ao consumir mensagens do RabbitMQ: %v", err)
	}


	// Start HTTP server
	http.HandleFunc("/resources/data", handleGetAllResourcesData(collection))
	http.HandleFunc("/resources/", handleGetResourceData(collection))

	go func() {
		log.Println("HTTP server listening on :8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("Failed to start HTTP server: %v", err)
		}
	}()

	// Configura o listener de mensagens
	go func() {
		for d := range msgs {
			log.Println("Nova mensagem:", string(d.Body))
			var ResourceData ResourceData
			err := json.Unmarshal(d.Body, &ResourceData)
			if err != nil {
				log.Printf("Erro ao decodificar mensagem JSON: %v", err)
				continue
			}

			// Insere os dados no MongoDB
			_, err = collection.InsertOne(ctx, bson.M{
				"uuid": ResourceData.SensorID,
				"data": ResourceData.Data,
			})
			if err != nil {
				log.Printf("Erro ao inserir dados no MongoDB: %v", err)
				continue
			}
			log.Printf("Dados armazenados para sensor_id: %s. Dados: %s", ResourceData.SensorID, ResourceData.Data)
		}
	}()

	// Aguarda por interrupções para encerrar o serviço
	log.Println("Aguardando mensagens. Pressione Ctrl+C para sair")
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Encerrando o serviço")
}
