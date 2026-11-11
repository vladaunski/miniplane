// Package nodeclient is the control plane's view of the node (the data plane).
package nodeclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"time"
)

// ErrPermanent marks a node rejection that retrying cannot fix, such as a stale
// generation or a tenant mismatch. The worker gives up on these immediately.
var ErrPermanent = errors.New("permanent node error")

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
func (c *Client) Create(ctx context.Context, vmID, tenant string, gen int64) error {
	body, err := json.Marshal(map[string]any{"tenant_id": tenant, "generation": gen})
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
func (c *Client) Delete(ctx context.Context, vmID, tenant string, gen int64) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		c.base+"/vms/"+url.PathEscape(vmID)+"?tenant_id="+url.QueryEscape(tenant)+
			"&generation="+strconv.FormatInt(gen, 10), nil)
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
	if slices.Contains(okStatuses, resp.StatusCode) {
		return nil
	}
	switch resp.StatusCode {
	case http.StatusBadRequest, http.StatusForbidden, http.StatusConflict:
		return fmt.Errorf("node returned %s: %w", resp.Status, ErrPermanent)
	default:
		return fmt.Errorf("node returned %s", resp.Status)
	}
}

// List returns the IDs of the VMs the node currently has.
func (c *Client) List(ctx context.Context) (map[string]bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.base+"/vms", nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("node request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("node returned %s", resp.Status)
	}
	var vms []struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&vms); err != nil {
		return nil, fmt.Errorf("decode node list: %w", err)
	}
	out := make(map[string]bool, len(vms))
	for _, v := range vms {
		out[v.ID] = true
	}
	return out, nil
}
