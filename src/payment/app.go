package main

import (
	"context"
	"encoding/json"
	"errors"
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

type DoneResponse struct {
	Done bool `json:"done"`
}

type PaidResponse struct {
	Paid bool `json:"paid"`
}

var client *mongo.Client
var userCollection *mongo.Collection
var paymentCollection *mongo.Collection

func main() {
	go shared.SetUpKafkaListener(
		[]string{"payment"}, false,
		func(message *shared.SagaMessage) (*shared.SagaMessage, string) {

			returnMessage := shared.SagaMessageConvertStartToEnd(message)

			// TODO: remove code duplication

			if message.Name == "START-MAKE-PAYMENT" {
				// ignore error, wil not happen
				_, mongoUserID := shared.ConvertStringToMongoID(message.Order.UserID)
				_, mongoOrderID := shared.ConvertStringToMongoID(message.Order.OrderID)

				clientError, serverError := pay(mongoUserID, mongoOrderID, &message.Order.TotalCost)
				if clientError != nil || serverError != nil {
					returnMessage.Name = "ABORT-CHECKOUT-SAGA"
				}
				return returnMessage, "payment-ack"
			}

			if message.Name == "START-CANCEL-PAYMENT" {
				// ignore error, wil not happen
				_, mongoUserID := shared.ConvertStringToMongoID(message.Order.UserID)
				_, mongoOrderID := shared.ConvertStringToMongoID(message.Order.OrderID)

				clientError, serverError := cancelPayment(mongoUserID, mongoOrderID)
				if clientError != nil || serverError != nil {
					returnMessage.Name = "ABORT-CHECKOUT-SAGA"
				}

				return returnMessage, "payment-ack"
			}

			return nil, ""
		},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var err error
	// client, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://paymentdb-svc-0:27017"))
	client, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Disconnect(ctx)

	db := client.Database("payment")
	userCollection = db.Collection("users")
	paymentCollection = db.Collection("payments")

	router := mux.NewRouter()
	router.HandleFunc("/payment/pay/{user_id}/{order_id}/{amount}", payHandler)
	router.HandleFunc("/payment/cancel/{user_id}/{order_id}", cancelPaymentHandler)
	router.HandleFunc("/payment/status/{user_id}/{order_id}", paymentStatusHandler)
	router.HandleFunc("/payment/add_funds/{user_id}/{amount}", addFundsHandler)
	router.HandleFunc("/payment/create_user", createUserHandler)
	router.HandleFunc("/payment/find_user/{user_id}", findUserHandler)

	port := os.Getenv("PORT")
	fmt.Printf("Current port is : %s\n", port)
	if port == "" {
		port = "8081"
	}

	// Set the listening address and port for the server
	addr := fmt.Sprintf(":%s", port)
	fmt.Printf("Starting payment service at %s\n", addr)
	log.Fatal(http.ListenAndServe(addr, router))
}

func getUser(documentID *primitive.ObjectID) (error, *shared.User) {
	return getUserWithContext(documentID, context.Background())
}

func getUserWithContext(documentID *primitive.ObjectID, ctx context.Context) (error, *shared.User) {
	var user shared.User
	err := userCollection.FindOne(ctx, bson.M{"_id": documentID}).Decode(&user)
	if err != nil {
		return err, nil
	}
	user.UserID = documentID.Hex()
	return nil, &user
}

func getPayment(userID *primitive.ObjectID, orderID *primitive.ObjectID) (error, *shared.Payment) {
	return getPaymentWithContext(userID, orderID, context.Background())
}

func getPaymentWithContext(userID *primitive.ObjectID, orderID *primitive.ObjectID, ctx context.Context) (error, *shared.Payment) {
	filter := bson.M{"userid": userID, "orderid": orderID}
	var payment shared.Payment
	findErr := paymentCollection.FindOne(ctx, filter).Decode(&payment)
	if findErr != nil {
		return findErr, nil
	}
	return nil, &payment
}

// Functions only used by http

func paymentStatusHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]
	orderID := vars["order_id"]

	userIdConvErr, mongoUserID := shared.ConvertStringToMongoID(userID)
	if userIdConvErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	orderIdConvErr, mongoOrderID := shared.ConvertStringToMongoID(orderID)
	if orderIdConvErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	filter := bson.M{"userid": mongoUserID, "orderid": mongoOrderID}
	var payment shared.Payment
	findErr := paymentCollection.FindOne(context.Background(), filter).Decode(&payment)
	if findErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	response := PaidResponse{
		Paid: payment.Paid,
	}
	w.Header().Set("Content-Type", "application/json")
	jsonErr := json.NewEncoder(w).Encode(response)
	if jsonErr != nil {
		http.Error(w, jsonErr.Error(), http.StatusInternalServerError)
		return
	}
}

func addFundsHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]
	amount := vars["amount"]

	idConvErr, documentID := shared.ConvertStringToMongoID(userID)
	if idConvErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	amountConvErr, amountFloat := shared.ConvertStringToFloat(amount)
	if amountConvErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	filter := bson.M{"_id": documentID}
	update := bson.M{
		"$inc": bson.M{
			"credit": amountFloat,
		},
	}
	_, updateErr := userCollection.UpdateOne(context.Background(), filter, update)
	response := DoneResponse{}
	if updateErr != nil {
		response.Done = false
	} else {
		response.Done = true
	}

	w.Header().Set("Content-Type", "application/json")
	jsonErr := json.NewEncoder(w).Encode(response)
	if jsonErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func createUserHandler(w http.ResponseWriter, r *http.Request) {
	user := shared.User{
		Credit: 0.0,
	}
	result, insertionError := userCollection.InsertOne(context.Background(), user)
	if insertionError != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	userID := result.InsertedID.(primitive.ObjectID).Hex()
	user.UserID = userID

	w.Header().Set("Content-Type", "application/json")
	jsonError := json.NewEncoder(w).Encode(user)
	if jsonError != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func findUserHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]

	userIdConvErr, mongoUserID := shared.ConvertStringToMongoID(userID)
	if userIdConvErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	userFindErr, user := getUser(mongoUserID)
	if userFindErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	jsonErr := json.NewEncoder(w).Encode(user)
	if jsonErr != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// Functions used by http and kafka

func payHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]
	orderID := vars["order_id"]
	amount := vars["amount"]

	userIdConvErr, mongoUserID := shared.ConvertStringToMongoID(userID)
	if userIdConvErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	orderIdConvErr, mongoOrderID := shared.ConvertStringToMongoID(orderID)
	if orderIdConvErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	amountConvErr, amountFloat := shared.ConvertStringToFloat(amount)
	if amountConvErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	clientError, serverError := pay(mongoUserID, mongoOrderID, amountFloat)

	if clientError != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if serverError != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func pay(userID *primitive.ObjectID, orderID *primitive.ObjectID, amount *float64) (clientError error, serverError error) {
	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		getuserErr, user := getUserWithContext(userID, sessCtx)
		if getuserErr != nil {
			return nil, getuserErr
		}
		if user.Credit < *amount {
			return nil, errors.New("not enough credits to pay")
		}

		userFilter := bson.M{
			"_id": userID,
		}
		userUpdate := bson.M{
			"$inc": bson.M{
				"credit": -*amount,
			},
		}
		_, userUpdateError := userCollection.UpdateOne(sessCtx, userFilter, userUpdate)
		if userUpdateError != nil {
			return nil, userUpdateError
		}

		payment := shared.Payment{
			UserID:  userID.Hex(),
			OrderID: orderID.Hex(),
			Amount:  *amount,
			Paid:    true,
		}
		_, insertErr := paymentCollection.InsertOne(sessCtx, payment)
		if insertErr != nil {
			return nil, insertErr
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

func cancelPaymentHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]
	orderID := vars["order_id"]

	userIdConvErr, mongoUserID := shared.ConvertStringToMongoID(userID)
	if userIdConvErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	orderIdConvErr, mongoOrderID := shared.ConvertStringToMongoID(orderID)
	if orderIdConvErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	clientError, serverError := cancelPayment(mongoUserID, mongoOrderID)

	if clientError != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	if serverError != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func cancelPayment(userID *primitive.ObjectID, orderID *primitive.ObjectID) (clientError error, serverError error) {
	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		getPaymentErr, payment := getPaymentWithContext(userID, orderID, sessCtx)
		if getPaymentErr != nil {
			return nil, getPaymentErr
		}

		userFilter := bson.M{
			"_id": userID,
		}
		userUpdate := bson.M{
			"$inc": bson.M{
				"credit": payment.Amount,
			},
		}
		_, userUpdateError := userCollection.UpdateOne(sessCtx, userFilter, userUpdate)
		if userUpdateError != nil {
			return nil, userUpdateError
		}

		paymentFilter := bson.M{
			"userid":  userID,
			"orderid": orderID,
		}
		paymentUpdate := bson.M{
			"$set": bson.M{
				"paid": false,
			},
		}
		_, paymentUpdateErr := paymentCollection.UpdateOne(sessCtx, paymentFilter, paymentUpdate)
		if paymentUpdateErr != nil {
			return nil, paymentUpdateErr
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
