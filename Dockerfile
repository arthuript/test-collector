FROM golang:1.19 AS builder

WORKDIR /app

COPY go.mod go.sum ./

RUN go mod download

COPY . .

# Build the application
RUN go build -o main .

# Use a minimal base image for the final container
FROM debian:bullseye-slim

WORKDIR /app

# Copy binary from the builder stage
COPY --from=builder /app/main .

# Expose application port
EXPOSE 8081

# Command to run the application
CMD ["./main"]
