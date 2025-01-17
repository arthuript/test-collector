package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

var collection *mongo.Collection

// SensorData é a estrutura do JSON que será recebida e armazenada (sem Lat/Lon)
type SensorData struct {
	Data struct {
		Temperature float64 `json:"temperature"`
		Humidity    float64 `json:"humidity"`
		Timestamp   string  `json:"timestamp"`
	} `json:"data"`
}

func main() {
	// Configura o cliente MongoDB
	client, err := mongo.NewClient(options.Client().ApplyURI("mongodb://mongo:27017"))
	if err != nil {
		log.Fatalf("Erro ao criar cliente MongoDB: %v", err)
	}

	// Conecta ao MongoDB
	ctx := context.Background()
	err = client.Connect(ctx)
	if err != nil {
		log.Fatalf("Erro ao conectar ao MongoDB: %v", err)
	}

	// Verifica se a conexão está funcionando
	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		log.Fatalf("Erro ao testar conexão com MongoDB: %v", err)
	}

	// Seleciona a coleção de sensores
	collection = client.Database("intercity").Collection("sensors")

	// Configuração dos endpoints HTTP
	http.HandleFunc("/api/", apiHandler)

	// Inicia o servidor HTTP
	port := "8080"
	log.Printf("Servidor iniciado na porta %s", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

// apiHandler manipula as rotas POST e GET
func apiHandler(w http.ResponseWriter, r *http.Request) {
	// Extrai o UUID do sensor da URL
	UUID := r.URL.Path[len("/api/"):]
	if UUID == "" {
		http.Error(w, "UUID não fornecido", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPost:
		handlePostData(UUID, w, r)
	case http.MethodGet:
		handleGetData(UUID, w, r)
	default:
		http.Error(w, "Método não permitido", http.StatusMethodNotAllowed)
	}
}

// handlePostData lida com o envio de dados dos sensores
func handlePostData(uuid string, w http.ResponseWriter, r *http.Request) {
	var data SensorData

	// Decodifica o JSON recebido
	err := json.NewDecoder(r.Body).Decode(&data)
	if err != nil {
		http.Error(w, fmt.Sprintf("Erro ao decodificar JSON: %v", err), http.StatusBadRequest)
		return
	}

	// Insere os dados no MongoDB
	_, err = collection.InsertOne(context.Background(), bson.M{
		"sensor_id": uuid,
		"data":      data.Data,
	})
	if err != nil {
		http.Error(w, fmt.Sprintf("Erro ao inserir dados no MongoDB: %v", err), http.StatusInternalServerError)
		return
	}

	// Responde com sucesso
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte("Dados do sensor armazenados com sucesso"))
}

// handleGetData lida com a consulta dos dados do sensor
func handleGetData(uuid string, w http.ResponseWriter, r *http.Request) {
	// Consulta os dados do MongoDB para o sensor especificado
	cursor, err := collection.Find(context.Background(), bson.M{"sensor_id": uuid})
	if err != nil {
		http.Error(w, fmt.Sprintf("Erro ao consultar dados no MongoDB: %v", err), http.StatusInternalServerError)
		return
	}
	defer cursor.Close(context.Background())

	var results []SensorData
	for cursor.Next(context.Background()) {
		var result SensorData
		err := cursor.Decode(&result)
		if err != nil {
			http.Error(w, fmt.Sprintf("Erro ao decodificar dados do MongoDB: %v", err), http.StatusInternalServerError)
			return
		}
		results = append(results, result)
	}

	// Verifica se não encontrou dados
	if len(results) == 0 {
		http.Error(w, "Nenhum dado encontrado para este sensor", http.StatusNotFound)
		return
	}

	// Codifica os resultados em JSON e envia como resposta
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(results)
}
