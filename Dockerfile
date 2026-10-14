# Контекст сборки: каталог services/ (нужен для replace на ../cbl-kit и ../cbl-protos).
# Сборка: docker build -f cbl-webhook-ingress/Dockerfile -t cbl/webhook-ingress:dev .
FROM golang:1.26-alpine AS build
WORKDIR /src
COPY cbl-kit/ /src/cbl-kit/
COPY cbl-protos/ /src/cbl-protos/
COPY cbl-webhook-ingress/ /src/cbl-webhook-ingress/
WORKDIR /src/cbl-webhook-ingress
RUN go mod download
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/webhook-ingress ./cmd/webhook-ingress

FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/webhook-ingress /app/webhook-ingress
COPY --from=build /src/cbl-webhook-ingress/migrations /app/migrations
EXPOSE 8060 9260
ENTRYPOINT ["/app/webhook-ingress"]
