package dashhttp

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/skip2/go-qrcode"
)

type lanCandidate struct {
	interfaceName string
	address       string
}

var errNoLANIPv4 = errors.New("no usable LAN IPv4 address found; set JOIN_HOST to override")

// pickLANIPv4 chooses an IPv4 address without consulting the machine's network state.
func pickLANIPv4(candidates []lanCandidate) (string, bool) {
	bestIP := ""
	bestPriority := -1

	for _, candidate := range candidates {
		interfaceName := strings.ToLower(candidate.interfaceName)
		if isVirtualInterface(interfaceName) {
			continue
		}

		ip := net.ParseIP(strings.TrimSpace(candidate.address))
		ip4 := ip.To4()
		if ip4 == nil || ip4.IsLoopback() || ip4.IsUnspecified() || ip4[0] == 169 && ip4[1] == 254 {
			continue
		}

		priority := 0
		switch {
		case ip4[0] == 192 && ip4[1] == 168:
			priority = 3
		case ip4[0] == 10:
			priority = 2
		case ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31:
			priority = 1
		}

		if priority > bestPriority {
			bestIP = ip4.String()
			bestPriority = priority
		}
	}

	return bestIP, bestIP != ""
}

func isVirtualInterface(name string) bool {
	for _, marker := range []string{
		"docker", "veth", "vethernet", "wsl", "virtualbox", "vmware",
		"hyper-v", "loopback", "bluetooth",
	} {
		if strings.Contains(name, marker) {
			return true
		}
	}
	return false
}

func detectLANIPv4() (string, error) {
	if host := strings.TrimSpace(os.Getenv("JOIN_HOST")); host != "" {
		return host, nil
	}

	interfaces, err := net.Interfaces()
	if err != nil {
		return "", fmt.Errorf("list network interfaces: %w", err)
	}

	candidates := make([]lanCandidate, 0)
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addresses, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, address := range addresses {
			var ip net.IP
			switch address := address.(type) {
			case *net.IPNet:
				ip = address.IP
			case *net.IPAddr:
				ip = address.IP
			}
			if ip != nil {
				candidates = append(candidates, lanCandidate{
					interfaceName: iface.Name,
					address:       ip.String(),
				})
			}
		}
	}

	ip, ok := pickLANIPv4(candidates)
	if !ok {
		return "", errNoLANIPv4
	}
	return ip, nil
}

func cleanPort(port string) string {
	port = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(port), ":"))
	if port == "" {
		return "8080"
	}
	return port
}

func buildJoinURL(port, host string) (string, error) {
	if strings.TrimSpace(host) == "" {
		return "", errNoLANIPv4
	}
	return fmt.Sprintf("http://%s:%s/worker", host, cleanPort(port)), nil
}

func writeJSONError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": message})
}

// RegisterQR registers the worker join URL and QR image endpoints.
func RegisterQR(mux *http.ServeMux, port string) {
	registerQR(mux, port, detectLANIPv4)
}

func registerQR(mux *http.ServeMux, port string, getHost func() (string, error)) {
	mux.HandleFunc("/api/joinurl", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		host, err := getHost()
		if err != nil {
			writeJSONError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		joinURL, err := buildJoinURL(port, host)
		if err != nil {
			writeJSONError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{"url": joinURL})
	})

	mux.HandleFunc("/qr.png", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}

		host, err := getHost()
		if err != nil {
			writeJSONError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		joinURL, err := buildJoinURL(port, host)
		if err != nil {
			writeJSONError(w, http.StatusServiceUnavailable, err.Error())
			return
		}
		png, err := qrcode.Encode(joinURL, qrcode.Medium, 512)
		if err != nil {
			writeJSONError(w, http.StatusInternalServerError, "failed to generate QR code")
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write(png); err != nil {
			return
		}
	})
}
