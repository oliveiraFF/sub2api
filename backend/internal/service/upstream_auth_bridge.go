package service

import (
	"net/http"

	"github.com/Wei-Shaw/sub2api/internal/upstream"
)

// Step 1 of the upstream-adapter migration: route the *auth header decision* for
// Anthropic API-key accounts through the adapter, leaving every other part of
// the request build path untouched.
//
// The adapter is state-free, so a single package-level instance is enough and
// the hot path allocates nothing extra.
var claudeUpstreamAuth = upstream.NewClaudeAdapter()

// anthropicAPIKeyCredentials projects the account-side auth decision onto the
// upstream credential shape. This is the only place that knows how an *Account*
// maps onto the adapter contract for the Claude platform.
func (a *Account) anthropicAPIKeyCredentials(token string) upstream.Credentials {
	creds := upstream.Credentials{
		Kind:   upstream.AuthAPIKey,
		APIKey: token,
	}
	if a == nil {
		return creds
	}
	if a.GetAnthropicAPIKeyAuthScheme() == AnthropicAPIKeyAuthSchemeAuthorizationBearer {
		creds.APIKeyScheme = upstream.APIKeySchemeAuthorizationBearer
	}
	return creds
}

// upstreamAuthHeaderNames lists the headers the bridge is allowed to copy out of
// an adapter header set.
//
// Deliberately excludes the adapter's other mandatory headers (anthropic-version,
// accept): the gateway sets anthropic-version conditionally so a client-provided
// value survives, and copying the adapter's value here would silently override
// that. Auth is the only concern being moved in this step.
var upstreamAuthHeaderNames = []string{"authorization", "x-api-key", "x-goog-api-key"}

// copyUpstreamAuthHeaders copies the adapter-chosen auth header onto dst. It
// never deletes: every current call site already strips inbound auth headers
// (authorization / x-api-key / x-goog-api-key / cookie) before calling the
// helper, so deleting here would be redundant work on the hot path.
func copyUpstreamAuthHeaders(dst, src http.Header) {
	for _, name := range upstreamAuthHeaderNames {
		if v := src.Get(name); v != "" {
			dst.Set(name, v)
			return
		}
	}
}
