package main

import (
	"log"
	"net/http"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
)

var allowedOrigins = map[string]bool{
	"http://localhost:8080": true,
	"http://127.0.0.1:8080": true,
	"http://127.0.0.1:5500": true,
	"http://localhost:5500": true,
}
var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,

	// CheckOrigin: func(r *http.Request) bool {
	// 	return r.Header.Get("Origin") == "http://localhost:8080"
	// },

	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		return allowedOrigins[origin]
	},
}

func main() {
	// Connect to NATS.
	nc, err := nats.Connect(
		nats.DefaultURL,
		nats.Name("realtime-server"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	hub := NewHub()

	// Subscribe to messages.
	_, err = nc.Subscribe("chat.room.general", func(msg *nats.Msg) {
		hub.Broadcast(msg.Data)
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := nc.Flush(); err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {

		log.Printf("Origin: %s", r.Header.Get("Origin"))
		log.Printf("Host: %s", r.Host)
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Println("WebSocket upgrade error:", err)
			return
		}

		client := &Client{
			conn: conn,
			send: make(chan []byte, 256),
			hub:  hub,
		}

		hub.Register(client)

		go client.WritePump()

		client.ReadPump(func(message []byte) error {
			return nc.Publish("chat.room.general", message)
		})
	})

	log.Println("WebSocket server listening on :8080")

	log.Fatal(http.ListenAndServe(":8080", nil))
}
