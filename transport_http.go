package main

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/go-kit/kit/endpoint"
)

func makeGetEndpoint(svc PingerService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(getRequest)
		job, err := svc.Get(req.Key)
		if err != nil {
			return getResponse{job, err.Error()}, nil
		}
		return getResponse{Job: job, Err: ""}, nil
	}
}

func makePutEndpoint(svc PingerService) endpoint.Endpoint {
	return func(ctx context.Context, request interface{}) (interface{}, error) {
		req := request.(putRequest)
		if err := svc.Put(req.NewJob); err != nil {
			return putResponse{Err: err.Error()}, nil
		}

		return putResponse{Err: ""}, nil
	}
}

func decodeGetRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var request getRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}

func decodePutRequest(_ context.Context, r *http.Request) (interface{}, error) {
	var request putRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return nil, err
	}
	return request, nil
}

func encodeResponse(_ context.Context, w http.ResponseWriter, response interface{}) error {
	return json.NewEncoder(w).Encode(response)
}

type getRequest struct {
	Key string `json:"key"`
}

type putRequest struct {
	NewJob *Job `json:"job"`
}

type putResponse struct {
	SavedJob *Job   `json:"job"`
	Err      string `json:"err, omitempty"`
}
type getResponse struct {
	Job *Job   `json:"job"`
	Err string `json:"err, omitempty"`
}
