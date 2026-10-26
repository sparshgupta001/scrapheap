package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"

	"github.com/scrap-heap/backend/internal/dashhttp"
	"github.com/scrap-heap/backend/internal/metrics"
)

func main() {
	addrDefault := os.Getenv("PORT")
	if addrDefault == "" {
		addrDefault = ":8080"
	} else if !strings.Contains(addrDefault, ":") {
		addrDefault = ":" + addrDefault
	}
	speedDefault := 1.0
	if speedEnv := os.Getenv("SPEED"); speedEnv != "" {
		parsed, err := strconv.ParseFloat(speedEnv, 64)
		if err != nil || parsed <= 0 {
			log.Fatalf("invalid SPEED %q: must be a positive number", speedEnv)
		}
		speedDefault = parsed
	}

	addr := flag.String("addr", addrDefault, "HTTP listen address (or PORT environment variable)")
	speed := flag.Float64("speed", speedDefault, "simulation speed multiplier")
	sched := flag.String("sched", "aware", "simulated scheduler mode: aware or pull")
	flag.Parse()
	if *speed <= 0 {
		log.Fatal("-speed must be greater than zero")
	}
	if !strings.Contains(*addr, ":") {
		*addr = ":" + *addr
	}
	if *sched != "aware" && *sched != "pull" {
		log.Fatal("-sched must be either aware or pull")
	}

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("listen on %s: %v", *addr, err)
	}

	collector := metrics.NewCollector()
	hub := metrics.NewHub(collector)
	defer hub.Close()
	sim := &simulator{collector: collector, speed: *speed, mode: *sched}
	defer sim.close()

	hub.OnRunAgain = func() {
		sim.start(false)
	}
	hub.OnRunAgainWithConfig = func(config string) {
		sim.start(strings.EqualFold(strings.TrimSpace(config), "B"))
	}

	mux := http.NewServeMux()
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	dashhttp.RegisterQR(mux, port)
	dashhttp.RegisterDash(mux, hub)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			http.Redirect(w, r, "/dash.html", http.StatusTemporaryRedirect)
			return
		}
		http.FileServer(http.Dir("web")).ServeHTTP(w, r)
	})

	fmt.Printf("Dashboard URL: http://localhost:%s/dash.html\n", port)
	printJoinURL(mux)
	fmt.Printf("Simulation speed: %gx\n", *speed)
	fmt.Printf("Scheduler mode: %s\n", *sched)

	sim.start(false)
	server := &http.Server{Handler: mux}
	if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
		log.Fatalf("serve dashboard: %v", err)
	}
}

func printJoinURL(handler http.Handler) {
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/joinurl", nil))
	if recorder.Code != http.StatusOK {
		fmt.Printf("Join URL unavailable: %s\n", strings.TrimSpace(recorder.Body.String()))
		return
	}
	var response struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		fmt.Printf("Join URL unavailable: invalid response: %v\n", err)
		return
	}
	fmt.Printf("Join URL: %s\n", response.URL)
}
