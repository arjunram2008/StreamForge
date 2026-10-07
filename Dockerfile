FROM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
COPY tests ./tests
RUN go test ./... && CGO_ENABLED=0 go build -o /out/broker ./cmd/broker

FROM debian:bookworm-slim
RUN useradd --create-home --uid 10001 streamforge && mkdir /data && chown streamforge:streamforge /data
COPY --from=build /out/broker /usr/local/bin/streamforge
USER streamforge
WORKDIR /app
EXPOSE 8080
ENTRYPOINT ["streamforge"]
CMD ["--addr", "0.0.0.0:8080", "--data", "/data", "--peers", "http://127.0.0.1:8080"]
