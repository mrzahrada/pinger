package main

import (
	"fmt"
	"os"
	"time"
)

func main() {

	nodeAddr := os.Getenv("PINGER_NODE_ADDR")
	existingAddr := os.Getenv("PINGER_EXISTING_ADDR")
	pingerDir := os.Getenv("PINGER_DIR")
	apiAddr := os.Getenv("PINGER_API_ADDR")
	_ = apiAddr
	//logger := log.NewLogfmtLogger(os.Stderr)

	svc, err := NewPinger(nodeAddr, existingAddr, pingerDir)
	if err != nil {
		panic(err)
	}
	//svc = loggingMiddleware{logger, svc}

	for {
		time.Sleep(3 * time.Second)
		job1 := NewRandomJob()
		err = svc.Put(job1)
		if err != nil {
			panic(err)
		}
		time.Sleep(time.Second)

		job, err := svc.Get(job1.Key())
		if err != nil {
			panic(err)
		}
		fmt.Println("job:", job)

	}

	/*
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
		http.Handle("/get", getHandler)
		http.Handle("/put", putHandler)
		//http.Handle("/metrics", promhttp.Handler())
		//logger.Log("msg", "HTTP", "addr", ":8080")
		logger.Log("err", http.ListenAndServe(apiAddr, nil))
	*/
}
