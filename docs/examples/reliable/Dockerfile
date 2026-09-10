# Build stage
FROM golang:1.25 AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -o /out/reliable ./docs/examples/reliable

# Runtime stage
FROM gcr.io/distroless/static-debian12

COPY --from=build /out/reliable /usr/local/bin/reliable

ENV QUEUE_NAME=reliable-amqp10-go-queue
ENV IS_SILENT=false
ENV MESSAGES_TO_SEND=500000
ENV ADDRESS=amqp://guest:guest@localhost:5672/

ENTRYPOINT ["/usr/local/bin/reliable"]
