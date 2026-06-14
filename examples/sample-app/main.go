package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/Saad7890-web/rhombus/pkg/rhombus"
	"github.com/jackc/pgx/v5/pgxpool"
)

type CreateOrderRequest struct {
	ID          string `json:"id"`
	CustomerID  string `json:"customer_id"`
	AmountCents int64  `json:"amount_cents"`
}

type CreateOrderResponse struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

func main() {
	ctx := context.Background()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("DATABASE_URL is required")
	}

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("failed to connect to postgres: %v", err)
	}
	defer pool.Close()

	if err := ensureSchema(ctx, pool); err != nil {
		log.Fatalf("failed to ensure schema: %v", err)
	}

	client, err := rhombus.New(pool)
	if err != nil {
		log.Fatalf("failed to create rhombus client: %v", err)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
	})

	mux.HandleFunc("/orders", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		var req CreateOrderRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		if req.ID == "" || req.CustomerID == "" || req.AmountCents <= 0 {
			http.Error(w, "missing or invalid fields", http.StatusBadRequest)
			return
		}

		payload, err := json.Marshal(map[string]any{
			"order_id":     req.ID,
			"customer_id":  req.CustomerID,
			"amount_cents": req.AmountCents,
		})
		if err != nil {
			http.Error(w, "failed to build payload", http.StatusInternalServerError)
			return
		}

		err = client.WithTransaction(r.Context(), func(tx *rhombus.Transaction) error {
			_, err := tx.Exec(
				`INSERT INTO orders (id, customer_id, amount_cents) VALUES ($1, $2, $3)`,
				req.ID,
				req.CustomerID,
				req.AmountCents,
			)
			if err != nil {
				return err
			}

			return tx.EnqueueEvent(&rhombus.Event{
				AggregateType: "order",
				AggregateID:   req.ID,
				OrderingKey:   req.ID,
				EventType:     "orders.created",
				SchemaVersion: 1,
				Payload:       payload,
				Metadata:      []byte(`{"source":"sample-app"}`),
				Destination:   []byte(`{"kafka":{"topic":"orders.created"}}`),
				AvailableAt:   time.Now().UTC(),
			})
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(CreateOrderResponse{
			OrderID: req.ID,
			Status:  "created",
		})
	})

	addr := getenv("APP_ADDR", ":8090")
	log.Printf("sample app listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Fatal(err)
	}
}

func ensureSchema(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS orders (
			id TEXT PRIMARY KEY,
			customer_id TEXT NOT NULL,
			amount_cents BIGINT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)
	`)
	return err
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}