FROM golang:1.25-alpine

WORKDIR /app

COPY . .

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o app

CMD ["./app"]