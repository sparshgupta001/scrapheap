package dashhttp

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestPickLANIPv4PriorityAndFiltering(t *testing.T) {
	candidates := []lanCandidate{
		{interfaceName: "Loopback Pseudo-Interface", address: "192.168.1.2"},
		{interfaceName: "Ethernet", address: "127.0.0.1"},
		{interfaceName: "Ethernet", address: "169.254.4.5"},
		{interfaceName: "vEthernet (WSL)", address: "192.168.1.3"},
		{interfaceName: "Docker Desktop", address: "10.0.0.3"},
		{interfaceName: "Bluetooth Network", address: "192.168.1.4"},
		{interfaceName: "Ethernet", address: "::1"},
		{interfaceName: "Wi-Fi", address: "172.20.0.1"},
		{interfaceName: "Ethernet 2", address: "10.2.0.1"},
		{interfaceName: "Wi-Fi 2", address: "192.168.7.8"},
	}
	got, ok := pickLANIPv4(candidates)
	if !ok || got != "192.168.7.8" {
		t.Fatalf("pickLANIPv4() = (%q, %v), want (192.168.7.8, true)", got, ok)
	}
}

func TestPickLANIPv4RejectsUnavailableAddresses(t *testing.T) {
	candidates := []lanCandidate{
		{interfaceName: "Ethernet", address: "127.0.0.1"},
		{interfaceName: "Ethernet", address: "169.254.0.1"},
		{interfaceName: "Ethernet", address: "::1"},
		{interfaceName: "VMware Network Adapter", address: "10.0.0.1"},
		{interfaceName: "Ethernet", address: "0.0.0.0"},
	}
	if got, ok := pickLANIPv4(candidates); ok || got != "" {
		t.Fatalf("pickLANIPv4() = (%q, %v), want empty, false", got, ok)
	}
}

func TestDetectLANIPv4Override(t *testing.T) {
	t.Setenv("JOIN_HOST", "  192.168.42.99  ")
	got, err := detectLANIPv4()
	if err != nil {
		t.Fatalf("detectLANIPv4() error = %v", err)
	}
	if got != "192.168.42.99" {
		t.Fatalf("detectLANIPv4() = %q, want 192.168.42.99", got)
	}
}

func TestRegisterQREndpoints(t *testing.T) {
	t.Setenv("JOIN_HOST", "192.168.42.99")
	mux := http.NewServeMux()
	RegisterQR(mux, ":8080")

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/joinurl", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /api/joinurl status = %d, want 200: %s", rec.Code, rec.Body)
	}
	var response map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode /api/joinurl JSON: %v", err)
	}
	if response["url"] != "http://192.168.42.99:8080/worker" {
		t.Fatalf("join URL = %q, want http://192.168.42.99:8080/worker", response["url"])
	}

	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/qr.png", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /qr.png status = %d, want 200: %s", rec.Code, rec.Body)
	}
	if got := rec.Header().Get("Content-Type"); got != "image/png" {
		t.Errorf("Content-Type = %q, want image/png", got)
	}
	if got := rec.Header().Get("Cache-Control"); got != "no-store" {
		t.Errorf("Cache-Control = %q, want no-store", got)
	}
	pngMagic := []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'}
	if body := rec.Body.Bytes(); len(body) < len(pngMagic) || !bytes.Equal(body[:len(pngMagic)], pngMagic) {
		t.Fatalf("invalid PNG signature: %v", body[:min(len(body), len(pngMagic))])
	}
}

func TestBuildJoinURLWithoutHost(t *testing.T) {
	if _, err := buildJoinURL("8080", ""); err == nil {
		t.Fatal("buildJoinURL() expected error for missing LAN address")
	}

	mux := http.NewServeMux()
	registerQR(mux, "8080", func() (string, error) {
		return "", errNoLANIPv4
	})
	for _, path := range []string{"/api/joinurl", "/qr.png"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("GET %s without LAN status = %d, want 503", path, rec.Code)
		}
		var response map[string]string
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatalf("decode %s no-LAN response: %v", path, err)
		}
		if response["error"] == "" {
			t.Fatalf("%s no-LAN response has no explanatory error", path)
		}
	}
}

func TestRegisterQRRejectsNonGET(t *testing.T) {
	t.Setenv("JOIN_HOST", "192.168.1.5")
	mux := http.NewServeMux()
	RegisterQR(mux, "8080")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/api/joinurl", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /api/joinurl status = %d, want 405", rec.Code)
	}
	if got := rec.Header().Get("Allow"); got != http.MethodGet {
		t.Errorf("Allow = %q, want GET", got)
	}
}
