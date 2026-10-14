# AGENTS.md - cbl-webhook-ingress

Прием вебхуков провайдера: проверка HMAC по сырому телу, inbox, быстрый ответ.

Обязательные глобальные правила: `../../AGENTS.md`, `../../.specs/CBL-001-development-process.md`,
`../../.specs/CBL-002-repos-and-docker.md`, контракты `../../.specs/CBL-003-contracts.md`.
Длинное тире (U+2014) запрещено, используется только дефис.

## 1. Границы

In scope: POST /hooks/{provider}/{endpoint_id}, HMAC, inbox, outbox в gateway.webhooks.raw.

Out of scope: другие сервисы платформы; взаимодействие только через контракты `cbl-protos`
(gRPC), Kafka и REST периметра (см. `CBL-000`, раздел 2).

## 2. Контракты

- REST: POST /hooks/{provider}/{endpoint_id}
- gRPC: нет
- Kafka продюсирует: gateway.webhooks.raw
- Kafka потребляет: нет
- Хранилище: PostgreSQL webhook_ingress

Контракты заморожены в `cbl-protos` (CF-1 для gRPC, CF-2/CF-3 для событий), см. `CBL-004`.

## 3. Архитектура

Слои: `internal/transport` -> `internal/usecase` -> `internal/domain` -> `internal/storage`.
Библиотеки: стандартный набор `CBL-002` (Gin, GORM + pgx-драйвер, franz-go + pkg/sr, go-redis,
goose, zap). `AutoMigrate` запрещен, миграции только goose, только вперед.

## 4. Команды

```powershell
make build        # go build ./...
make test         # go test ./...
make vet          # go vet ./...
make lint         # golangci-lint в Docker
make up           # docker compose -f deploy/docker-compose.yml up -d
make down         # docker compose -f deploy/docker-compose.yml down -v
```

## 5. Порты и окружение

HTTP `8060`, gRPC `-`, метрики `9260`.
Переменные (применимые к сервису): `HTTP_ADDR`, `GRPC_ADDR`, `METRICS_ADDR`, `PG_DSN`,
`KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, `REDIS_ADDR`.

## 6. Правила разработки

- Конвейер: SDD-спека в `.specs/` -> архитектура -> контракты -> тесты вперед -> код ->
  ревью 4 агентами (без контекста, архитектура, контекст, безопасность - блокирующий) ->
  AGENTS.md -> финальные прогоны -> линтеры -> коммит.
- Покрытие unit+integration не ниже 80%; денежные сценарии только на реальных PostgreSQL и
  Kafka (testcontainers); локальный e2e в docker-compose обязателен.
- Деньги только int64 в минимальных единицах плюс decimals; float запрещен.
- Docstring на русском в стиле godoc (первая строка с имени сущности, точка в конце);
  комментарии объясняют почему; ошибки через `%w`; панки в бизнес-коде запрещены.
- Коммит: `CBL-<эпик>.<номер>: <краткое сообщение>`, спека с тем же префиксом в том же коммите.

## 7. SLO и метрики

SLO уточняется в задаче 14.4. Базовые метрики: latency p95/p99, доля ошибок, outbox lag,
consumer lag; дашборды после задачи 0.7.

## 8. Trade-offs

| ID | Решение | Плюсы | Минусы и компенсация |
| :-- | :-- | :-- | :-- |
| T-WH-01 | Ответ дубликата расширен полем `duplicate` | клиенту видно повтор без повторного эффекта | расширение контракта | зафиксировано в CBL-5.1 FR-3 |
| T-WH-02 | `event_id` - детерминированный UUIDv5 от provider и vendor id | дедупликация без таблицы соответствий, inbox требует UUID | vendor id обязан быть уникальным на каждое событие | зафиксировано в FR-4a; при переиспользовании id включить статус в ключ (5.2) |
| T-WH-03 | В событие попадают только первые значения заголовков, чувствительные отброшены | безопасность и предсказуемость | многозначные заголовки теряются | политика уточняется в 5.3 |

## 9. Known issues

| ID | Проблема | Влияние | План | Статус |
| :-- | :-- | :-- | :-- | :-- |
| KI-webhook-ingress-01 | CI и pre-commit не настроены | линтеры запускаются вручную | задача 0.2 | open |
| KI-webhook-ingress-02 | Нагрузочный профиль (5.4) не написан | порог p99 менее 500 мс не подтвержден | задача 5.4 | open |
| KI-webhook-ingress-03 | traceparent не пробрасывается в конверт события | сквозная трассировка асинхронного звена неполная | задача 0.7 | open |

## 10. Журнал задач

| CBL | Дата | Статус | Спека | Коммит |
| :-- | :-- | :-- | :-- | :-- |
| CBL-5.1 | 2026-10-14 | done, ревью закрыто (HMAC-эндпоинт, allowlist, e2e с Kafka, покрытие 84.5%) | `.specs/CBL-5.1-hmac-endpoint.md` | ожидает |

## 11. Runbook

Заполняется первой продуктовой задачей сервиса. Типовые аварии: недоступность зависимости,
рост ошибок, лаг обработки; действия: проверить health/metrics, логи с request_id, состояние
Kafka consumer group и outbox lag, при необходимости перезапустить сервис.
