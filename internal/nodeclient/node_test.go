package nodeclient

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRejectionsAreClassified(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		permanent bool
	}{
		{"stale generation", http.StatusConflict, true},
		{"wrong tenant", http.StatusForbidden, true},
		{"bad request", http.StatusBadRequest, true},
		{"node broken", http.StatusInternalServerError, false},
		{"node overloaded", http.StatusServiceUnavailable, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
			}))
			defer srv.Close()

			c := New(srv.URL)
			for name, err := range map[string]error{
				"create": c.Create(context.Background(), "vm1", "acme", 1),
				"delete": c.Delete(context.Background(), "vm1", "acme", 1),
			} {
				if err == nil {
					t.Fatalf("%s: want an error for status %d", name, tc.status)
				}
				if got := errors.Is(err, ErrPermanent); got != tc.permanent {
					t.Errorf("%s: permanent = %v, want %v (%v)", name, got, tc.permanent, err)
				}
			}
		})
	}
}

func TestSuccessIsNotAnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	c := New(srv.URL)
	if err := c.Create(context.Background(), "vm1", "acme", 1); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := c.Delete(context.Background(), "vm1", "acme", 2); err != nil {
		t.Fatalf("delete: %v", err)
	}
}
