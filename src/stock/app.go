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

	"WDM-G1/shared"
	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type ItemChange struct {
	itemID *primitive.ObjectID
	amount int64
}

var client *mongo.Client
var stockCollection *mongo.Collection

func main() {
	go shared.SetUpKafkaListener(
		[]string{"stock"}, false,
		func(message *shared.SagaMessage) (*shared.SagaMessage, string) {

			returnMessage := shared.SagaMessageConvertStartToEnd(message)

			// TODO: remove code duplication

			if message.Name == "START-SUBTRACT-STOCK" {
				changes := make([]ItemChange, len(message.Order.Items))

				for i, stringID := range message.Order.Items {
					// ignore error, will not happen
					_, itemID := shared.ConvertStringToMongoID(stringID)

					changes[i] = ItemChange{
						itemID: itemID,
						amount: 1,
					}
				}

				clientError, serverError := subtract(changes)
				if clientError != nil || serverError != nil {
					returnMessage.Name = "ABORT-CHECKOUT-SAGA"
				}

				return returnMessage, "stock-ack"
			}

			if message.Name == "START-READD-STOCK" {
				changes := make([]ItemChange, len(message.Order.Items))

				for i, stringID := range message.Order.Items {
					// ignore error, will not happen
					_, itemID := shared.ConvertStringToMongoID(stringID)

					changes[i] = ItemChange{
						itemID: itemID,
						amount: 1,
					}
				}

				clientError, serverError := add(changes)
				if clientError != nil || serverError != nil {
					returnMessage.Name = "ABORT-CHECKOUT-SAGA"
				}

				return returnMessage, "stock-ack"
			}

			return nil, ""
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var err error
	// TODO: implement hash
	// client, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://stockdb-svc-0:27017"))
	client, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
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

func getItem(documentID *primitive.ObjectID) (error, *shared.Item) {
	return getItemWithContext(documentID, context.Background())
}

func getItemWithContext(documentID *primitive.ObjectID, ctx context.Context) (error, *shared.Item) {
	var item shared.Item
	err := stockCollection.FindOne(ctx, bson.M{"_id": documentID}).Decode(&item)
	if err != nil {
		return err, nil
	}
	item.ItemID = documentID.Hex()
	return nil, &item
}

// Functions only used by http

func findHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	itemID := vars["item_id"]
	// fmt.Printf("item ID: %s", itemID)

	convertDocIDErr, documentID := shared.ConvertStringToMongoID(itemID)
	if convertDocIDErr != nil {
		fmt.Println("CONVERT DOC ERROR")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// fmt.Printf("Find: %s\n", itemID)
	findErr, item := getItem(documentID)
	if findErr != nil {
		fmt.Println("GET ITEM ERROR")
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	jsonEncodeErr := json.NewEncoder(w).Encode(item)
	if jsonEncodeErr != nil {
		fmt.Println("JSON ENCODE ERROR")
		w.WriteHeader(http.StatusBadRequest)
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
	// fmt.Printf("Creating item with price %s\n", price)
	stock := shared.Item{
		Stock: 0,
		Price: priceFloat,
	}
	result, insertErr := stockCollection.InsertOne(context.Background(), stock)
	if insertErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	stockID := result.InsertedID.(primitive.ObjectID).Hex()
	// fmt.Printf("Created a new item with ID: %s\n", stockID)
	stock.ItemID = stockID

	w.Header().Set("Content-Type", "application/json")
	jsonEncodeErr := json.NewEncoder(w).Encode(stock)
	if jsonEncodeErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
}

// Functions used by http and kafka

func subtractHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	itemID := vars["item_id"]
	amount := vars["amount"]
	convertIntErr, intAmount := shared.ConvertStringToInt(amount)
	if convertIntErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	convertDocIDErr, documentID := shared.ConvertStringToMongoID(itemID)
	if convertDocIDErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	clientError, serverError := subtract([]ItemChange{{
		itemID: documentID,
		amount: *intAmount,
	}})

	if clientError != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if serverError != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func subtract(changes []ItemChange) (clientError error, serverError error) {
	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		for _, change := range changes {
			getItemErr, item := getItemWithContext(change.itemID, sessCtx)
			if getItemErr != nil {
				return nil, getItemErr
			}
			if item.Stock < change.amount {
				return nil, errors.New("not enough stock to subtract")
			}
			update := bson.M{
				"$inc": bson.M{
					"stock": -change.amount,
				},
			}
			_, updateErr := stockCollection.UpdateOne(sessCtx, bson.M{"_id": change.itemID}, update)
			if updateErr != nil {
				// fmt.Printf("Update stock error: %s", updateErr)
				return nil, updateErr
			}
		}
		return nil, nil
	}

	var session mongo.Session
	session, serverError = client.StartSession()
	if serverError != nil {
		return
	}

	ctx := context.Background()
	defer session.EndSession(ctx)

	_, clientError = session.WithTransaction(ctx, callback)

	return
}

func addHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	itemID := vars["item_id"]
	amount := vars["amount"]
	convIntErr, intAmount := shared.ConvertStringToInt(amount)
	if convIntErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	convStringErr, documentID := shared.ConvertStringToMongoID(itemID)
	if convStringErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	clientError, serverError := add([]ItemChange{{
		itemID: documentID,
		amount: *intAmount,
	}})

	if clientError != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if serverError != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func add(changes []ItemChange) (clientError error, serverError error) {
	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		for _, change := range changes {
			filter := bson.M{"_id": change.itemID}
			update := bson.M{
				"$inc": bson.M{
					"stock": change.amount,
				},
			}
			_, updateError := stockCollection.UpdateOne(sessCtx, filter, update)
			if updateError != nil {
				return nil, updateError
			}
		}
		return nil, nil
	}

	var session mongo.Session
	session, serverError = client.StartSession()
	if serverError != nil {
		return
	}

	ctx := context.Background()
	defer session.EndSession(ctx)

	_, clientError = session.WithTransaction(ctx, callback)

	return
}
