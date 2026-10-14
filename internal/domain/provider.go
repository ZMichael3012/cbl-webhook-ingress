// Пакет domain содержит модель настроек приема вебхуков провайдеров.
package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidConfig сообщает о некорректной конфигурации провайдеров.
var ErrInvalidConfig = errors.New("webhook-ingress: invalid provider config")

// ProviderConfig описывает настройки приема вебхуков одного провайдера.
type ProviderConfig struct {
	// Name задает имя провайдера (ключ конфигурации).
	Name string `json:"-"`
	// SignatureHeader задает имя заголовка подписи.
	SignatureHeader string `json:"signature_header"`
	// EventIDHeader задает имя заголовка идентификатора события.
	EventIDHeader string `json:"event_id_header"`
	// SecretEnv задает имя переменной окружения с секретом подписи.
	SecretEnv string `json:"secret_env"`
	// Endpoints задает allowlist допустимых endpoint_id; пустое значение разрешает любые.
	Endpoints []string `json:"endpoints"`
	// Secret задает секрет подписи, подставленный из окружения.
	Secret []byte `json:"-"`
}

// ParseProviders разбирает JSON-конфигурацию провайдеров вида
// {"mock":{"signature_header":"X-Signature","event_id_header":"X-Event-Id",
// "secret_env":"MOCK_WEBHOOK_SECRET"}} и подставляет секреты через lookupEnv.
func ParseProviders(raw string, lookupEnv func(string) string) (map[string]ProviderConfig, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("%w: empty config", ErrInvalidConfig)
	}
	var parsed map[string]ProviderConfig
	if err := json.Unmarshal([]byte(raw), &parsed); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidConfig, err)
	}
	if len(parsed) == 0 {
		return nil, fmt.Errorf("%w: no providers", ErrInvalidConfig)
	}
	providers := make(map[string]ProviderConfig, len(parsed))
	for name, config := range parsed {
		config.Name = name
		if strings.TrimSpace(config.SignatureHeader) == "" ||
			strings.TrimSpace(config.EventIDHeader) == "" ||
			strings.TrimSpace(config.SecretEnv) == "" {
			return nil, fmt.Errorf("%w: provider %q has empty fields", ErrInvalidConfig, name)
		}
		secret := lookupEnv(config.SecretEnv)
		if secret == "" {
			return nil, fmt.Errorf("%w: secret env %q is empty", ErrInvalidConfig, config.SecretEnv)
		}
		config.Secret = []byte(secret)
		providers[name] = config
	}
	return providers, nil
}
