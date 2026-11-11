// Package nodeclient is the control plane's view of the node (the data plane).
package nodeclient

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"
)

// Client talks to one node over HTTP.
type Client struct {
	base string
	hc   *http.Client
}

// New returns a Client for the node at base, e.g. "http://127.0.0.1:9001".
func New(base string) *Client {
	return &Client{
		base: strings.TrimRight(base, "/"),
		hc:   &http.Client{Timeout: 5 * time.Second},
	}
}

// Create asks the node to ensure the VM exists. Safe to repeat: the node is idempotent.
func (c *Client) Create(ctx context.Context, vmID, tenant string) error {
	body, err := json.Marshal(map[string]string{"tenant_id": tenant})
	if err != nil {
		return fmt.Errorf("encode body: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		c.base+"/vms/"+url.PathEscape(vmID), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	return c.do(req, http.StatusOK, http.StatusCreated)
}

// Delete asks the node to ensure the VM is gone. Safe to repeat.
func (c *Client) Delete(ctx context.Context, vmID, tenant string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		c.base+"/vms/"+url.PathEscape(vmID)+"?tenant_id="+url.QueryEscape(tenant), nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	return c.do(req, http.StatusNoContent)
}

func (c *Client) do(req *http.Request, okStatuses ...int) error {
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("node request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if !slices.Contains(okStatuses, resp.StatusCode) {
		return fmt.Errorf("node returned %s", resp.Status)
	}
	return nil
}
