package dashhttp

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestSelectPreferredIP(t *testing.T) {
	tests := []struct {
		name       string
		candidates []string
		want       string
	}{
		{
			name:       "empty candidates defaults to loopback",
			candidates: []string{},
			want:       "127.0.0.1",
		},
		{
			name:       "loopback only candidates defaults to loopback",
			candidates: []string{"127.0.0.1", "::1"},
			want:       "127.0.0.1",
		},
		{
			name:       "prefers 192.168 over 10.x and 172.x",
			candidates: []string{"172.20.10.4", "10.0.0.15", "192.168.1.105"},
			want:       "192.168.1.105",
		},
		{
			name:       "prefers 10.x over 172.16-31",
			candidates: []string{"172.25.0.2", "10.50.1.20"},
			want:       "10.50.1.20",
		},
		{
			name:       "prefers 172.16-31 over general public IP",
			candidates: []string{"8.8.8.8", "172.18.0.5"},
			want:       "172.18.0.5",
		},
		{
			name:       "falls back to any IPv4 if no RFC1918 match",
			candidates: []string{"100.64.0.1"},
			want:       "100.64.0.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := SelectPreferredIP(tt.candidates)
			if got != tt.want {
				t.Errorf("SelectPreferredIP(%v) = %v, want %v", tt.candidates, got, tt.want)
			}
		})
	}
}

func TestDetectLANIPOverride(t *testing.T) {
	orig := os.Getenv("JOIN_HOST")
	defer os.Setenv("JOIN_HOST", orig)

	os.Setenv("JOIN_HOST", "192.168.42.99")
	if got := DetectLANIP(); got != "192.168.42.99" {
		t.Errorf("expected JOIN_HOST override '192.168.42.99', got %s", got)
	}
}

func TestRegisterQREndpoints(t *testing.T) {
	mux := http.NewServeMux()
	RegisterQR(mux, "8080")

	// 1. Test /api/joinurl
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/joinurl", nil)
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected /api/joinurl status 200, got %d", rec.Code)
	}
	var res map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatalf("failed to decode /api/joinurl JSON: %v", err)
	}
	urlVal, ok := res["url"]
	if !ok || urlVal == "" {
		t.Fatalf("expected non-empty 'url' in response, got %v", res)
	}

	// 2. Test /qr.png
	recQR := httptest.NewRecorder()
	reqQR := httptest.NewRequest(http.MethodGet, "/qr.png", nil)
	mux.ServeHTTP(recQR, reqQR)

	if recQR.Code != http.StatusOK {
		t.Fatalf("expected /qr.png status 200, got %d", recQR.Code)
	}
	if ct := recQR.Header().Get("Content-Type"); ct != "image/png" {
		t.Errorf("expected Content-Type 'image/png', got %s", ct)
	}
	body := recQR.Body.Bytes()
	if len(body) < 8 {
		t.Fatalf("QR png response too short (%d bytes)", len(body))
	}
	pngMagic := []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}
	if !bytes.Equal(body[:8], pngMagic) {
		t.Errorf("invalid PNG header: got %v, want %v", body[:8], pngMagic)
	}
}
