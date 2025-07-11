FROM golang:1.24 AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o poc example/main.go

FROM gcr.io/distroless/base-debian12
WORKDIR /root/
COPY --from=build /app/poc ./poc
CMD ["/root/poc"]
