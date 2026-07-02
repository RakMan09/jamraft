# Build the self-contained node binary (web UI is embedded via go:embed).
FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -o /jamnode ./cmd/node

FROM gcr.io/distroless/static-debian12
COPY --from=build /jamnode /jamnode
EXPOSE 7000 8080
ENTRYPOINT ["/jamnode"]
