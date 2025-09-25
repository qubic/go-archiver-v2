FROM golang:1.25 AS builder
ENV CGO_ENABLED=0

WORKDIR /src
COPY . /src

RUN go mod tidy
RUN go build -o "/src/bin/go-archiver-v2"

FROM alpine:latest
LABEL authors="mio@qubic.org"

COPY --from=builder /src/bin/go-archiver-v2 /app/go-archiver-v2
RUN chmod +x /app/go-archiver-v2

WORKDIR /app

ENTRYPOINT ["./go-archiver-v2"]
