package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

func main() {
	h := &handler{Port: os.Getenv("PORT")}

	http.Handle("/logs", h)
	log.Printf("Starting drain on PORT=%s\n", h.Port)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%s", h.Port), nil))
}

type handler struct {
	Port string
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
	}
	log.Printf("PORT=%s LOG=%s", h.Port, body)
	defer r.Body.Close()

	w.WriteHeader(http.StatusOK)
}
