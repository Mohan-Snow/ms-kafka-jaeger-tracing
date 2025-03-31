FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY . .

RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o producer-service .

FROM alpine:latest
WORKDIR /app

COPY --from=builder /app/producer-service .

EXPOSE 8080
CMD ["./producer-service"]