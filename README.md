# cbl-webhook-ingress

Прием вебхуков провайдера: проверка HMAC по сырому телу, inbox, быстрый ответ.

Роль сервиса и контракты: `AGENTS.md`. Глобальные правила платформы: `../../AGENTS.md`,
`../../.specs/CBL-000-system-description.md`, `../../.specs/CBL-003-contracts.md`.

## Команды

```powershell
make build        # сборка
make test         # мокнутые unit-тесты
make vet          # go vet
make lint         # golangci-lint в Docker
make up           # зависимости + сервис в docker-compose
make down
```
