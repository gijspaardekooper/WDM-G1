FROM golang:1.20.4 AS BUILD

WORKDIR /app

COPY go.mod go.sum app.go utils.go ./

RUN go mod download && \
    go build -o main

# TODO: Why are two stages needed? (The base image is the same)
FROM golang:1.20.4

WORKDIR /app

COPY --from=build /app/main .

ENV PORT 5000

EXPOSE $PORT

ENTRYPOINT ["./main"]
