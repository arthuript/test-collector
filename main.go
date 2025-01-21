package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/streadway/amqp"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var collection *mongo.Collection

// ResourceData é a estrutura do JSON que será recebida e armazenada
type ResourceData struct {
	SensorID string                 `json:"sensor_id"`
	Data     map[string]interface{} `json:"data"`
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
	if err != nil {
		log.Fatalf("Erro ao conectar ao RabbitMQ: %v", err)
	}
	defer conn.Close()
	log.Println("Conectado ao RabbitMQ com sucesso")

	// Cria um canal no RabbitMQ
	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Erro ao abrir um canal no RabbitMQ: %v", err)
	}
	defer ch.Close()

	// Declara a fila de mensagens (tópico)
	queueName := "data_stream"
	_, err = ch.QueueDeclare(
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

	// Configura o listener de mensagens
	go func() {
		for d := range msgs {
			var ResourceData ResourceData
			err := json.Unmarshal(d.Body, &ResourceData)
			if err != nil {
				log.Printf("Erro ao decodificar mensagem JSON: %v", err)
				continue
			}

			// Insere os dados no MongoDB
			_, err = collection.InsertOne(ctx, bson.M{
				"sensor_id": ResourceData.SensorID,
				"data":      ResourceData.Data,
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
