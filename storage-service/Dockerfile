FROM golang:1.24-alpine AS builder

WORKDIR /app
COPY . .

# Install build dependencies for PostgreSQL
RUN apk add --no-cache gcc musl-dev

RUN go mod download
RUN CGO_ENABLED=1 GOOS=linux go build -o storage-service .

FROM alpine:latest
WORKDIR /app

# Required for PostgreSQL driver
RUN apk add --no-cache libc6-compat postgresql-client

COPY --from=builder /app/storage-service .

EXPOSE 8082
CMD ["./storage-service"]