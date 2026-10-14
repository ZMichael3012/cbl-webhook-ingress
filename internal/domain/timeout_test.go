package domain_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cbl/cbl-webhook-ingress/internal/domain"
)

func TestParseWriteTimeout(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		raw     string
		want    time.Duration
		wantErr bool
	}{
		"default on empty":      {raw: "", want: domain.DefaultWriteTimeout},
		"default on spaces":     {raw: "  ", want: domain.DefaultWriteTimeout},
		"explicit value":        {raw: "300ms", want: 300 * time.Millisecond},
		"clamp to minimum":      {raw: "10ms", want: domain.MinWriteTimeout},
		"clamp to maximum":      {raw: "1h", want: domain.MaxWriteTimeout},
		"invalid syntax":        {raw: "soon", wantErr: true},
		"non-positive rejected": {raw: "-1s", wantErr: true},
		"zero rejected":         {raw: "0s", wantErr: true},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := domain.ParseWriteTimeout(tc.raw)
			if tc.wantErr {
				require.ErrorIs(t, err, domain.ErrInvalidWriteTimeout)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
