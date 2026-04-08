FROM golang:1.22 AS builder

WORKDIR /app
COPY . .

RUN go mod tidy
RUN CGO_ENABLED=0 GOOS=linux go build -o app

FROM alpine:latest

WORKDIR /root/
COPY --from=builder /app/app .

CMD ["./app"]