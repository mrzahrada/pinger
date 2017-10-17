package main

import (
	"log"
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {

	nodeAddr := os.Getenv("PINGER_NODE_ADDR")
	existingAddr := os.Getenv("PINGER_EXISTING_ADDR")
	pingerDir := os.Getenv("PINGER_DIR")

	logger := log.NewLogfmtLogger(os.Stderr)

	svc, err := NewPinger(nodeAddr, existingAddr, pingerDir)
	if err != nil {
		panic(err)
	}
	svc = loggingMiddleware{logger, svc}

	getHandler := httptransport.NewServer(
		makeGetEndpoint(svc),
		decodeGetRequest,
		encodeResponse,
	)

	putHandler := httptransport.NewServer(
		makePutEndpoint(svc),
		decodePutRequest,
		encodeResponse,
	)

	// TODO: this looks stupid, use rest methods
	http.Handle("/get", uppercaseHandler)
	http.Handle("/put", countHandler)
	http.Handle("/metrics", promhttp.Handler())
	logger.Log("msg", "HTTP", "addr", ":8080")
	logger.Log("err", http.ListenAndServe(":8080", nil))

}
