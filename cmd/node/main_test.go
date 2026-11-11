package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNodeRefusesStaleCreateAfterDelete(t *testing.T) {
	s := &server{vms: map[string]vm{}, seen: map[string]stamp{}}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /vms/{id}", s.put)
	mux.HandleFunc("DELETE /vms/{id}", s.delete)
	call := func(method, target, body string) int {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(method, target, strings.NewReader(body)))
		return rec.Code
	}

	if c := call("PUT", "/vms/x", `{"tenant_id":"acme","generation":1}`); c != http.StatusCreated {
		t.Fatalf("create = %d", c)
	}
	if c := call("DELETE", "/vms/x?tenant_id=acme&generation=2", ""); c != http.StatusNoContent {
		t.Fatalf("delete = %d", c)
	}
	// The late create from a stalled worker arrives after the delete.
	if c := call("PUT", "/vms/x", `{"tenant_id":"acme","generation":1}`); c != http.StatusConflict {
		t.Fatalf("stale create = %d, want 409", c)
	}
	if len(s.vms) != 0 {
		t.Fatal("stale create resurrected the VM")
	}
	// A retry of the delete itself is still fine.
	if c := call("DELETE", "/vms/x?tenant_id=acme&generation=2", ""); c != http.StatusNoContent {
		t.Fatalf("delete retry = %d", c)
	}
}
