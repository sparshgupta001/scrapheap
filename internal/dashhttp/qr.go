package dashhttp

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"

	"github.com/skip2/go-qrcode"
)

// SelectPreferredIP returns the highest-priority IPv4 address from a candidate list.
// Priority:
// 1. 192.168.x.x
// 2. 10.x.x.x
// 3. 172.16.0.0 - 172.31.255.255
// 4. Any other non-loopback IPv4
// 5. Fallback: "127.0.0.1"
func SelectPreferredIP(candidates []string) string {
	bestScore := -1
	bestIP := "127.0.0.1"

	for _, raw := range candidates {
		ip := net.ParseIP(strings.TrimSpace(raw))
		if ip == nil {
			continue
		}
		ip4 := ip.To4()
		if ip4 == nil || ip4.IsLoopback() {
			continue
		}

		score := 40 // general IPv4
		b0 := ip4[0]
		b1 := ip4[1]

		if b0 == 192 && b1 == 168 {
			score = 100
		} else if b0 == 10 {
			score = 80
		} else if b0 == 172 && (b1 >= 16 && b1 <= 31) {
			score = 60
		}

		if score > bestScore {
			bestScore = score
			bestIP = ip4.String()
		}
	}

	return bestIP
}

func isVirtualInterface(name string) bool {
	lower := strings.ToLower(name)
	virtualNames := []string{
		"docker", "veth", "wsl", "hyper-v", "virtual", "vbox",
		"vmnet", "virbr", "tap", "tun", "vethernet",
	}
	for _, v := range virtualNames {
		if strings.Contains(lower, v) {
			return true
		}
	}
	return false
}

// DetectLANIP discovers the primary local area network IPv4 address.
// If the environment variable JOIN_HOST is set, it takes precedence.
func DetectLANIP() string {
	if env := os.Getenv("JOIN_HOST"); env != "" {
		return strings.TrimSpace(env)
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return "127.0.0.1"
	}

	var candidates []string
	for _, iface := range ifaces {
		// Skip interfaces that are down, loopback, or virtual/docker adapters
		if (iface.Flags&net.FlagUp) == 0 || (iface.Flags&net.FlagLoopback) != 0 {
			continue
		}
		if isVirtualInterface(iface.Name) {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}
		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip != nil && ip.To4() != nil && !ip.IsLoopback() {
				candidates = append(candidates, ip.To4().String())
			}
		}
	}

	return SelectPreferredIP(candidates)
}

// CleanPort strips leading colons and defaults to "8080".
func CleanPort(port string) string {
	p := strings.TrimPrefix(strings.TrimSpace(port), ":")
	if p == "" {
		p = "8080"
	}
	return p
}

// BuildJoinURL constructs the worker join URL for the given host/port.
func BuildJoinURL(port string) string {
	host := DetectLANIP()
	p := CleanPort(port)
	return fmt.Sprintf("http://%s:%s/worker", host, p)
}

// RegisterQR registers the /api/joinurl and /qr.png endpoints on the provided mux.
func RegisterQR(mux *http.ServeMux, port string) {
	mux.HandleFunc("/api/joinurl", func(w http.ResponseWriter, r *http.Request) {
		joinURL := BuildJoinURL(port)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"url": joinURL,
		})
	})

	mux.HandleFunc("/qr.png", func(w http.ResponseWriter, r *http.Request) {
		joinURL := BuildJoinURL(port)
		png, err := qrcode.Encode(joinURL, qrcode.Medium, 512)
		if err != nil {
			http.Error(w, "failed to generate qr code", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(png)
	})
}
