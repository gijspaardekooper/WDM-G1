package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"WDM-G1/shared"
	"github.com/gorilla/mux"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var client *mongo.Client
var ordersCollection *mongo.Collection

func main() {
	go shared.SetUpKafkaListener(
		[]string{"order"}, false,
		func(message *shared.SagaMessage) (*shared.SagaMessage, string) {

			returnMessage := shared.SagaMessageConvertStartToEnd(message)

			if message.Name == "START-UPDATE-ORDER" {
				// ignore error, will not happen
				_, orderID := shared.ConvertStringToMongoID(message.Order.OrderID)

				clientError, serverError := updateOrder(orderID, true)
				if clientError != nil || serverError != nil {
					returnMessage.Name = "ABORT-CHECKOUT-SAGA"
				}

				return returnMessage, "order-ack"
			}

			return nil, ""
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var err error
	// TODO: implement hash
	// client, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://orderdb-svc-0:27017"))
	client, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))

	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	db := client.Database("orders")
	ordersCollection = db.Collection("orders")

	router := mux.NewRouter()
	router.HandleFunc("/orders/create/{user_id}", createOrderHandler)
	router.HandleFunc("/orders/remove/{order_id}", removeOrderHandler)
	router.HandleFunc("/orders/find/{order_id}", findOrderHandler)
	router.HandleFunc("/orders/addItem/{order_id}/{item_id}", addItemHandler)
	router.HandleFunc("/orders/removeItem/{order_id}/{item_id}", removeItemHandler)
	router.HandleFunc("/orders/checkout/{order_id}", checkoutHandler)

	port := os.Getenv("PORT")
	fmt.Printf("Current port is : %s\n", port)
	if port == "" {
		port = "8080"
	}

	// Set the listening address and port for the server
	addr := fmt.Sprintf(":%s", port)
	fmt.Printf("Starting order service at %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, router))
}

func getOrder(orderID *primitive.ObjectID) (error, *shared.Order) {
	return getOrderWithContext(orderID, context.Background())
}

func getOrderWithContext(orderID *primitive.ObjectID, ctx context.Context) (error, *shared.Order) {
	filter := bson.M{"_id": orderID}
	var order shared.Order
	findDocErr := ordersCollection.FindOne(ctx, filter).Decode(&order)
	if findDocErr != nil {
		return findDocErr, nil
	}
	order.OrderID = orderID.Hex()
	return nil, &order
}

// Functions only used by http

func createOrderHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]

	convertUserIDErr, mongoUserID := shared.ConvertStringToMongoID(userID)
	if convertUserIDErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	order := shared.Order{
		Paid:      false,
		Items:     []string{},
		UserID:    mongoUserID.Hex(),
		TotalCost: 0.0,
	}

	insertResult, mongoInsertErr := ordersCollection.InsertOne(context.Background(), order)
	if mongoInsertErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	orderID := insertResult.InsertedID.(primitive.ObjectID).Hex()
	order.OrderID = orderID

	w.Header().Set("Content-Type", "application/json")
	jsonEncodeErr := json.NewEncoder(w).Encode(order)
	if jsonEncodeErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func removeOrderHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	orderID := vars["order_id"]

	convertDocIDErr, documentID := shared.ConvertStringToMongoID(orderID)
	if convertDocIDErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	filter := bson.M{"_id": documentID}
	_, removeDocErr := ordersCollection.DeleteOne(context.Background(), filter)
	if removeDocErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
}

func findOrderHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	orderID := vars["order_id"]

	convertDocIDErr, documentID := shared.ConvertStringToMongoID(orderID)
	if convertDocIDErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	findOrderErr, order := getOrder(documentID)
	order.OrderID = orderID
	if findOrderErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	jsonEncodeErr := json.NewEncoder(w).Encode(order)
	if jsonEncodeErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func addItemHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	orderID := vars["order_id"]
	itemID := vars["item_id"]

	// fmt.Printf("Adding item %s to order %s", itemID, orderID)
	convertItemIDErr, mongoItemID := shared.ConvertStringToMongoID(itemID)
	if convertItemIDErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// TODO: use kafka

	getStockResponse, getStockErr := http.Get(fmt.Sprintf("http://localhost:8082/stock/find/%s", mongoItemID.Hex()))
	// fmt.Printf("response: %s", getStockResponse.StatusCode)
	// fmt.Printf("get stock err: %s", getStockErr)
	if getStockErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer getStockResponse.Body.Close()

	var item shared.Item
	jsonDecodeErr := json.NewDecoder(getStockResponse.Body).Decode(&item)
	if jsonDecodeErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	convertOrderIDErr, mongoOrderID := shared.ConvertStringToMongoID(orderID)
	if convertOrderIDErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	orderFilter := bson.M{"_id": mongoOrderID}
	orderUpdate := bson.M{
		"$push": bson.M{
			"items": mongoItemID.Hex(),
		},
		"$inc": bson.M{
			"totalcost": item.Price,
		},
	}
	_, addItemErr := ordersCollection.UpdateOne(context.Background(), orderFilter, orderUpdate)
	if addItemErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func removeItemHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	orderID := vars["order_id"]
	itemID := vars["item_id"]

	convertItemIDErr, mongoItemID := shared.ConvertStringToMongoID(itemID)
	if convertItemIDErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// TODO: use kafka

	getStockResponse, getStockErr := http.Get(fmt.Sprintf("http://localhost:8082/stock/find/%s", mongoItemID.Hex()))
	if getStockErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	defer getStockResponse.Body.Close()

	var item shared.Item
	jsonDecodeErr := json.NewDecoder(getStockResponse.Body).Decode(&item)
	if jsonDecodeErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	convertOrderIDErr, mongoOrderID := shared.ConvertStringToMongoID(orderID)
	if convertOrderIDErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	orderFilter := bson.M{"_id": mongoOrderID}
	orderUpdate := bson.M{
		"$pull": bson.M{
			"items": itemID,
		},
		"$inc": bson.M{
			"totalcost": -item.Price,
		},
	}
	_, removeItemErr := ordersCollection.UpdateOne(context.Background(), orderFilter, orderUpdate)
	if removeItemErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
}

func checkoutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	orderID := vars["order_id"]

	convertOrderIDErr, mongoOrderID := shared.ConvertStringToMongoID(orderID)
	if convertOrderIDErr != nil {
		fmt.Println("Convert String to Mongo ID error")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	getOrderErr, order := getOrder(mongoOrderID)
	order.OrderID = orderID
	// fmt.Println("Order ID", orderID, order.OrderID)
	if getOrderErr != nil {
		fmt.Println("Get order error")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	sender := shared.CreateTopicSender("order-ack")
	defer sender.Close()

	message := shared.SagaMessage{
		Name:   "START-CHECKOUT-SAGA",
		SagaID: -1,
		Order:  *order,
	}
	// message.Order.OrderID = orderID

	sendErr := shared.SendSagaMessage(&message, sender)
	if sendErr != nil {
		fmt.Println("Send Kafka SAGA message error")
		w.WriteHeader(http.StatusInternalServerError)
	}

	// TODO: wait for response and return status
	fmt.Println("TODO TODO TODO")
}

// Functions used only by kafka

func updateOrder(orderID *primitive.ObjectID, status bool) (clientError error, serverError error) {
	orderFilter := bson.M{"_id": orderID}
	orderUpdate := bson.M{
		"$set": bson.M{
			"paid": status,
		},
	}

	_, clientError = ordersCollection.UpdateOne(context.Background(), orderFilter, orderUpdate)

	return
}
