package http

import (
	"encoding/json"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/MaksProg/order-service/internal/cache"
	"github.com/MaksProg/order-service/internal/db"
)

func Router(store *db.Store, c *cache.Orders, staticDir string) http.Handler {
	r := chi.NewRouter()

	// GET /orders/{id}
	r.Get("/orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		id := chi.URLParam(r, "id")
		if id == "" {
			http.Error(w, "missing id", http.StatusBadRequest); return
		}
		if o, ok := c.Get(id); ok {
			_ = json.NewEncoder(w).Encode(o); return
		}
		o, err := store.GetOrder(r.Context(), id)
		if err != nil {
			http.Error(w, "not found", http.StatusNotFound); return
		}
		_ = json.NewEncoder(w).Encode(o)
	})

	// статика (простая страница)
	fs := http.FileServer(http.Dir(staticDir))
	r.Handle("/*", fs)
	return r
}
