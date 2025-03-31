FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY . .

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o consumer-service .

FROM alpine:latest
WORKDIR /app

COPY --from=builder /app/consumer-service .

EXPOSE 8081
CMD ["./consumer-service"]