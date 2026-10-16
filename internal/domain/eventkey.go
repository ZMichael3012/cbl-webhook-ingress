package domain

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

// ErrMissingEventKey reports a delivery without a resolvable vendor event key.
var ErrMissingEventKey = errors.New("webhook-ingress: missing vendor event key")

// keyPart is a compiled template segment: a literal or a dotted JSON path.
type keyPart struct {
	literal string
	path    []string
}

// VendorEventKey resolves the vendor event key of a delivery: the configured
// header, a template over the JSON body, or the legacy event_id_header for
// configurations without an event_key block. The key feeds the deterministic
// event_id (CBL-5.1 FR-4a) and therefore must not be empty.
func (c ProviderConfig) VendorEventKey(header http.Header, body []byte) (string, error) {
	headerName := strings.TrimSpace(c.EventIDHeader)
	template := ""
	if c.EventKey != nil {
		switch {
		case strings.TrimSpace(c.EventKey.Header) != "":
			headerName = strings.TrimSpace(c.EventKey.Header)
		case strings.TrimSpace(c.EventKey.Template) != "":
			headerName = ""
			template = strings.TrimSpace(c.EventKey.Template)
		}
	}
	if headerName != "" {
		value := strings.TrimSpace(header.Get(headerName))
		if value == "" {
			return "", fmt.Errorf("%w: header %q is empty", ErrMissingEventKey, headerName)
		}
		return value, nil
	}
	if template == "" {
		return "", fmt.Errorf("%w: provider %q has no event key source", ErrMissingEventKey, c.Name)
	}
	parts, err := compileTemplate(template)
	if err != nil {
		return "", err
	}
	parsed, err := decodeObject(body)
	if err != nil {
		return "", fmt.Errorf("%w: body: %v", ErrMissingEventKey, err)
	}
	var builder strings.Builder
	for _, part := range parts {
		if part.literal != "" {
			builder.WriteString(part.literal)
			continue
		}
		value, err := lookupScalar(parsed, part.path)
		if err != nil {
			return "", err
		}
		builder.WriteString(value)
	}
	return builder.String(), nil
}

// compileTemplate splits "{a.b}:{c}" into literal and path parts; a template
// without placeholders or with an empty placeholder is invalid, because a
// constant key would merge unrelated events into one dedup slot.
func compileTemplate(template string) ([]keyPart, error) {
	var parts []keyPart
	rest := template
	for len(rest) > 0 {
		open := strings.IndexByte(rest, '{')
		if open < 0 {
			parts = append(parts, keyPart{literal: rest})
			break
		}
		if open > 0 {
			parts = append(parts, keyPart{literal: rest[:open]})
		}
		closing := strings.IndexByte(rest[open:], '}')
		if closing < 0 {
			return nil, fmt.Errorf("%w: template %q has an unclosed placeholder", ErrMissingEventKey, template)
		}
		path := strings.TrimSpace(rest[open+1 : open+closing])
		if path == "" {
			return nil, fmt.Errorf("%w: template %q has an empty placeholder", ErrMissingEventKey, template)
		}
		parts = append(parts, keyPart{path: strings.Split(path, ".")})
		rest = rest[open+closing+1:]
	}
	hasPath := false
	for _, part := range parts {
		if len(part.path) > 0 {
			hasPath = true
		}
	}
	if !hasPath {
		return nil, fmt.Errorf("%w: template %q has no placeholders", ErrMissingEventKey, template)
	}
	return parts, nil
}

// lookupScalar resolves a dotted path over decoded JSON objects; string and
// json.Number values render as their literal text, and every segment must
// resolve to a non-empty scalar so that a broken body cannot collapse dedup keys.
func lookupScalar(object map[string]any, path []string) (string, error) {
	var current any = object
	for _, segment := range path {
		object, ok := current.(map[string]any)
		if !ok {
			return "", fmt.Errorf("%w: path %q is not an object", ErrMissingEventKey, strings.Join(path, "."))
		}
		value, ok := object[segment]
		if !ok {
			return "", fmt.Errorf("%w: path %q is missing", ErrMissingEventKey, strings.Join(path, "."))
		}
		current = value
	}
	switch value := current.(type) {
	case string:
		value = strings.TrimSpace(value)
		if value == "" {
			return "", fmt.Errorf("%w: path %q resolved empty", ErrMissingEventKey, strings.Join(path, "."))
		}
		return value, nil
	case json.Number:
		return value.String(), nil
	default:
		return "", fmt.Errorf("%w: path %q is not a scalar", ErrMissingEventKey, strings.Join(path, "."))
	}
}

// decodeObject decodes a JSON object with number literals preserved.
func decodeObject(body []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var object map[string]any
	if err := decoder.Decode(&object); err != nil {
		return nil, err
	}
	if object == nil {
		return nil, errors.New("body is not a JSON object")
	}
	return object, nil
}
