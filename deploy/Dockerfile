# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/openwar ./cmd/gateway \
    && go build -ldflags="-s -w" -o /out/worker ./cmd/worker \
    && go build -ldflags="-s -w" -o /out/backend-demo ./cmd/backend-demo

FROM alpine:3.20
RUN adduser -D -u 10001 appuser
USER appuser
COPY --from=build /out/ /app/
EXPOSE 8080
ENTRYPOINT ["/app/openwar"]
CMD ["serve"]