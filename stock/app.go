package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Item struct {
	StockID string  `json:"item_id"`
	Stock   int64   `json:"stock"`
	Price   float64 `json:"price"`
}

var client *mongo.Client
var stockCollection *mongo.Collection

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var err error
	client, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://mongodb-stock-service:27017"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	db := client.Database("stock")
	stockCollection = db.Collection("stock")

	router := mux.NewRouter()
	router.HandleFunc("/stock/find/{item_id}", findHandler)
	router.HandleFunc("/stock/subtract/{item_id}/{amount}", subtractHandler)
	router.HandleFunc("/stock/add/{item_id}/{amount}", addHandler)
	router.HandleFunc("/stock/item/create/{price}", createHandler)

	port := os.Getenv("PORT")
	fmt.Printf("Current port is: %s\n", port)
	if port == "" {
		port = "8082"
	}

	// Set the listening address and port for the server
	addr := fmt.Sprintf(":%s", port)
	fmt.Printf("Starting stock service at %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, router))
}

func getItem(itemID string) (error, *Item) {
	convErr, documentID := ConvertStringToMongoID(itemID)
	if convErr != nil {
		return convErr, nil
	}
	var item Item
	err := stockCollection.FindOne(context.Background(), bson.M{"_id": documentID}).Decode(&item)
	if err != nil {
		return err, nil
	}
	item.StockID = itemID
	return nil, &item
}

func findHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	itemID := vars["item_id"]

	findErr, item := getItem(itemID)
	if findErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	jsonEncodeErr := json.NewEncoder(w).Encode(item)
	if jsonEncodeErr != nil {
		w.WriteHeader(http.StatusBadRequest)
	}
}

func subtractHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	itemID := vars["item_id"]
	amount := vars["amount"]
	convertIntErr, intAmount := ConvertStringToInt(amount)
	if convertIntErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	convertDocIDErr, documentID := ConvertStringToMongoID(itemID)
	if convertDocIDErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		getItemErr, item := getItem(itemID)
		if getItemErr != nil {
			// fmt.Printf("Get item error")
			w.WriteHeader(http.StatusBadRequest)
			return nil, getItemErr
		}
		if item.Stock < *intAmount {
			// fmt.Printf("Not enough stock")
			return nil, errors.New("not enough stock to subtract")
		}
		update := bson.M{
			"$inc": bson.M{
				"stock": -*intAmount,
			},
		}
		_, updateErr := stockCollection.UpdateOne(context.Background(), bson.M{"_id": documentID}, update)
		if updateErr != nil {
			// fmt.Printf("Update stock error: %s", updateErr)
			return nil, updateErr
		}
		return nil, nil
	}

	session, startSessionErr := client.StartSession()
	// fmt.Printf("Started session")
	if startSessionErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	ctx := context.Background()
	defer session.EndSession(ctx)
	_, sessionWithTransactionErr := session.WithTransaction(ctx, callback)
	if sessionWithTransactionErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
}

func addHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	itemID := vars["item_id"]
	amount := vars["amount"]
	convIntErr, intAmount := ConvertStringToInt(amount)
	if convIntErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	convStringErr, documentID := ConvertStringToMongoID(itemID)
	if convStringErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	filter := bson.M{"_id": documentID}
	update := bson.M{
		"$inc": bson.M{
			"stock": intAmount,
		},
	}
	_, updateErr := stockCollection.UpdateOne(context.Background(), filter, update)
	for updateErr != nil {
		_, updateErr = stockCollection.UpdateOne(context.Background(), filter, update)
	}
}

func createHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	price := vars["price"]
	priceFloat, err := strconv.ParseFloat(price, 64)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	stock := Item{
		Stock: 0,
		Price: priceFloat,
	}
	result, insertErr := stockCollection.InsertOne(context.Background(), stock)
	if insertErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	stockID := result.InsertedID.(primitive.ObjectID).Hex()
	stock.StockID = stockID

	w.Header().Set("Content-Type", "application/json")
	jsonEncodeErr := json.NewEncoder(w).Encode(stock)
	if jsonEncodeErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
}
