FROM migrate/migrate:v4.18.3 AS migrate
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /worker ./cmd/catalog-comparison
FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /worker /worker
COPY --from=migrate /usr/local/bin/migrate /usr/local/bin/migrate
COPY migrations/ /migrations/
COPY run-migrations.sh /run-migrations
ENTRYPOINT ["/worker"]
