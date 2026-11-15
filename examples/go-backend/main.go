package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

type request struct {
	ClientID  string `json:"clientId"`
	SessionID string `json:"sessionId"`
	AgentID   string `json:"agentId"`
	RuntimeID string `json:"runtimeId,omitempty"`
	EventID   string `json:"eventId"`
	Content   string `json:"content"`
}

type taskReceipt struct {
	RunID     string `json:"runId"`
	Duplicate bool   `json:"duplicate"`
	Status    string `json:"status"`
}

func run() error {
	var payload request
	mode := flag.String("mode", "send", "send a notification or start a task (send or task)")
	rawURL := flag.String("gateway", "http://127.0.0.1:3001", "gateway base URL")
	flag.StringVar(&payload.ClientID, "client", "", "gateway client ID")
	flag.StringVar(&payload.SessionID, "session", "", "existing conversation ID")
	flag.StringVar(&payload.AgentID, "agent", "", "bound agent ID")
	flag.StringVar(&payload.RuntimeID, "runtime", "", "bound runtime ID (optional)")
	flag.StringVar(&payload.EventID, "event", "", "stable business event ID")
	flag.StringVar(&payload.Content, "content", "", "notification text or task prompt")
	flag.Parse()

	route, wantStatus := "api/send", http.StatusOK
	switch *mode {
	case "send":
	case "task":
		route, wantStatus = "api/tasks", http.StatusAccepted
	default:
		return fmt.Errorf("-mode must be send or task")
	}
	if flag.NArg() != 0 || strings.TrimSpace(payload.ClientID) == "" ||
		strings.TrimSpace(payload.SessionID) == "" || strings.TrimSpace(payload.AgentID) == "" ||
		strings.TrimSpace(payload.EventID) == "" || strings.TrimSpace(payload.Content) == "" {
		return fmt.Errorf("-client, -session, -agent, -event and -content are required")
	}

	token := strings.TrimSpace(os.Getenv("PUSH_CHANNEL_PUSH_TOKEN"))
	if token == "" {
		return fmt.Errorf("set PUSH_CHANNEL_PUSH_TOKEN to the gateway's server token")
	}
	gateway, err := url.Parse(*rawURL)
	if err != nil || (gateway.Scheme != "http" && gateway.Scheme != "https") ||
		gateway.Hostname() == "" || gateway.User != nil || gateway.RawQuery != "" || gateway.Fragment != "" {
		return fmt.Errorf("-gateway must be an HTTP or HTTPS base URL without credentials, query or fragment")
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, gateway.JoinPath(route).String(), bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build gateway request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	// Keep the authenticated request on the configured gateway URL.
	client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("gateway request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		return fmt.Errorf("POST /%s returned HTTP %d (expected %d)", route, resp.StatusCode, wantStatus)
	}
	if *mode == "send" {
		fmt.Println("Notification accepted (HTTP 200)")
		return nil
	}

	var receipt taskReceipt
	if err := json.NewDecoder(resp.Body).Decode(&receipt); err != nil {
		return fmt.Errorf("decode task receipt: %w", err)
	}
	if receipt.RunID == "" || receipt.Status == "" {
		return fmt.Errorf("task receipt is missing runId or status")
	}
	return json.NewEncoder(os.Stdout).Encode(receipt)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
