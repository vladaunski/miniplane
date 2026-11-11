// Command node is the fake data plane: it keeps VMs in memory.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

// stamp is the node's memory of a VM ID: the highest generation seen and its owner.
// It outlives the VM itself (a tombstone), so late commands for a deleted VM are refused.
type stamp struct {
	gen    int64
	tenant string
}

type vm struct {
	ID       string `json:"id"`
	TenantID string `json:"tenant_id"`
}

type server struct {
	mu   sync.Mutex
	vms  map[string]vm
	seen map[string]stamp
}

func newServer() *server {
	return &server{vms: map[string]vm{}, seen: map[string]stamp{}}
}

// routes wires the handlers; main and the tests share it so they cannot drift apart.
func (s *server) routes() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /vms/{id}", s.put)
	mux.HandleFunc("DELETE /vms/{id}", s.delete)
	mux.HandleFunc("GET /vms", s.list)
	return mux
}

// put handles PUT /vms/{id}. Idempotent, and refuses commands older than what it has seen.
func (s *server) put(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	r.Body = http.MaxBytesReader(w, r.Body, 1<<10)
	var req struct {
		TenantID   string `json:"tenant_id"`
		Generation int64  `json:"generation"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.TenantID == "" || req.Generation < 1 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.seen[id]; ok {
		if st.tenant != req.TenantID {
			http.Error(w, "conflict", http.StatusConflict)
			return
		}
		if req.Generation < st.gen {
			http.Error(w, "stale generation", http.StatusConflict)
			return
		}
	}
	s.seen[id] = stamp{gen: max(req.Generation, s.seen[id].gen), tenant: req.TenantID}

	if _, ok := s.vms[id]; ok {
		w.WriteHeader(http.StatusOK)
		return
	}
	s.vms[id] = vm{ID: id, TenantID: req.TenantID}
	w.WriteHeader(http.StatusCreated)
}

// delete handles DELETE /vms/{id}?tenant_id=...&generation=... Idempotent, and records a tombstone.
func (s *server) delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tenant := r.URL.Query().Get("tenant_id")
	gen, err := strconv.ParseInt(r.URL.Query().Get("generation"), 10, 64)
	if tenant == "" || err != nil || gen < 1 {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if st, ok := s.seen[id]; ok {
		if st.tenant != tenant {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if gen < st.gen {
			http.Error(w, "stale generation", http.StatusConflict)
			return
		}
	}
	s.seen[id] = stamp{gen: max(gen, s.seen[id].gen), tenant: tenant}
	delete(s.vms, id)
	w.WriteHeader(http.StatusNoContent)
}

// list handles GET /vms: lets callers observe what actually exists.
func (s *server) list(w http.ResponseWriter, _ *http.Request) {
	s.mu.Lock()
	out := make([]vm, 0, len(s.vms))
	for _, v := range s.vms {
		out = append(out, v)
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(out); err != nil {
		log.Printf("encode: %v", err)
	}
}

func main() {
	s := newServer()
	srv := &http.Server{
		Addr:              "127.0.0.1:9001", // localhost only until the node authenticates callers
		Handler:           s.routes(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Println("node listening on", srv.Addr)
	log.Fatal(srv.ListenAndServe())
}
