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
	"main/shared"
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
				fmt.Printf("STARTING PAYMENT!\n")
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
<<<<<<< HEAD:src/payment/app.go
	//TODO: implement hash
=======
>>>>>>> main:payment/app.go
	client, err = mongo.Connect(ctx, options.Client().ApplyURI("mongodb://paymentdb-svc-0:27017"))
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

<<<<<<< HEAD:src/payment/app.go
func getUser(documentID *primitive.ObjectID) (error, *shared.User) {
=======
func getUser(userID string) (error, *shared.User) {
	convErr, documentID := shared.ConvertStringToMongoID(userID)
	if convErr != nil {
		return convErr, nil
	}

>>>>>>> main:payment/app.go
	var user shared.User
	err := userCollection.FindOne(context.Background(), bson.M{"_id": documentID}).Decode(&user)
	if err != nil {
		return err, nil
	}
	user.UserID = documentID.String()
	return nil, &user
}

<<<<<<< HEAD:src/payment/app.go
func getPayment(userID *primitive.ObjectID, orderID *primitive.ObjectID) (error, *shared.Payment) {
	filter := bson.M{"userid": userID, "orderid": orderID}
=======
func getPayment(userID string, orderID string) (error, *shared.Payment) {
	userIdConvErr, mongoUserID := shared.ConvertStringToMongoID(userID)
	if userIdConvErr != nil {
		return userIdConvErr, nil
	}
	orderIdConvErr, mongoOrderID := shared.ConvertStringToMongoID(orderID)
	if orderIdConvErr != nil {
		return orderIdConvErr, nil
	}

	filter := bson.M{"userid": mongoUserID, "orderid": mongoOrderID}
>>>>>>> main:payment/app.go
	var payment shared.Payment
	findErr := paymentCollection.FindOne(context.Background(), filter).Decode(&payment)
	if findErr != nil {
		return findErr, nil
	}
	return nil, &payment
}

<<<<<<< HEAD:src/payment/app.go
// Functions only used by http
=======
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
	amountConvErr, amountFloat := shared.ConvertStringToFloat(amount)
	if amountConvErr != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		getuserErr, user := getUser(userID)
		if getuserErr != nil {
			// fmt.Printf("Get item error")
			w.WriteHeader(http.StatusBadRequest)
			return nil, getuserErr
		}
		if user.Credit < *amountFloat {
			// fmt.Printf("Not enough stock")
			return nil, errors.New("not enough credits to pay")
		}

		userFilter := bson.M{
			"_id": mongoUserID,
		}
		userUpdate := bson.M{
			"$inc": bson.M{
				"credit": -*amountFloat,
			},
		}
		_, userUpdateError := userCollection.UpdateOne(context.Background(), userFilter, userUpdate)
		if userUpdateError != nil {
			return nil, userUpdateError
		}

		payment := shared.Payment{
			UserID:  userID,
			OrderID: orderID,
			Amount:  *amountFloat,
			Paid:    true,
		}
		_, insertErr := paymentCollection.InsertOne(context.Background(), payment)
		if insertErr != nil {
			return nil, insertErr
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

func cancelPaymentHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	userID := vars["user_id"]
	orderID := vars["order_id"]

	// TODO: send kafka message to cancel order

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

	callback := func(sessCtx mongo.SessionContext) (interface{}, error) {
		getPaymentErr, payment := getPayment(userID, orderID)
		if getPaymentErr != nil {
			return nil, getPaymentErr
		}

		userFilter := bson.M{
			"_id": mongoUserID,
		}
		userUpdate := bson.M{
			"$inc": bson.M{
				"credit": payment.Amount,
			},
		}
		_, userUpdateError := userCollection.UpdateOne(context.Background(), userFilter, userUpdate)
		if userUpdateError != nil {
			return nil, userUpdateError
		}

		paymentFilter := bson.M{
			"userid":  mongoUserID,
			"orderid": mongoOrderID,
		}
		paymentUpdate := bson.M{
			"$set": bson.M{
				"paid": false,
			},
		}
		_, paymentUpdateErr := paymentCollection.UpdateOne(context.Background(), paymentFilter, paymentUpdate)
		if paymentUpdateErr != nil {
			return nil, paymentUpdateErr
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
>>>>>>> main:payment/app.go

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
		getuserErr, user := getUser(userID)
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
		_, userUpdateError := userCollection.UpdateOne(context.Background(), userFilter, userUpdate)
		if userUpdateError != nil {
			return nil, userUpdateError
		}

		payment := shared.Payment{
			UserID:  userID.String(),
			OrderID: orderID.String(),
			Amount:  *amount,
			Paid:    true,
		}
		_, insertErr := paymentCollection.InsertOne(context.Background(), payment)
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

	// TODO: send kafka message to cancel order

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
		getPaymentErr, payment := getPayment(userID, orderID)
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
		_, userUpdateError := userCollection.UpdateOne(context.Background(), userFilter, userUpdate)
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
		_, paymentUpdateErr := paymentCollection.UpdateOne(context.Background(), paymentFilter, paymentUpdate)
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