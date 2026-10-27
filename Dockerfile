FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . .
RUN CGO_ENABLED=0 go build -o /worker ./cmd/catalog-comparison
FROM alpine:3.21
RUN apk add --no-cache ca-certificates
COPY --from=build /worker /worker
ENTRYPOINT ["/worker"]
