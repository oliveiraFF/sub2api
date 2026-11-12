package service

import (
	"net/http"
	"reflect"
	"testing"
)

// legacySetAnthropicAPIKeyAuthHeader is the pre-adapter implementation, kept
// verbatim as the parity oracle for TestSetAnthropicAPIKeyAuthHeaderMatchesLegacy.
// If the adapter ever diverges from the historical wire format, this test fails
// before any gateway code path notices.
func legacySetAnthropicAPIKeyAuthHeader(header http.Header, account *Account, token string) {
	if account.GetAnthropicAPIKeyAuthScheme() == AnthropicAPIKeyAuthSchemeAuthorizationBearer {
		header.Set("Authorization", "Bearer "+token)
		return
	}
	header.Set("x-api-key", token)
}

func TestSetAnthropicAPIKeyAuthHeaderMatchesLegacy(t *testing.T) {
	accounts := map[string]*Account{
		"anthropic api_key": {
			Platform: PlatformAnthropic,
			Type:     AccountTypeAPIKey,
		},
		"anthropic api_key bearer relay": {
			Platform: PlatformAnthropic,
			Type:     AccountTypeAPIKey,
			Extra:    map[string]any{anthropicAPIKeyAuthSchemeExtraKey: AnthropicAPIKeyAuthSchemeAuthorizationBearer},
		},
		"nil account": nil,
		"non-anthropic platform": {
			Platform: PlatformOpenAI,
			Type:     AccountTypeAPIKey,
			Extra:    map[string]any{anthropicAPIKeyAuthSchemeExtraKey: AnthropicAPIKeyAuthSchemeAuthorizationBearer},
		},
	}
	tokens := []string{"sk-ant-api03-abc", ""}

	for name, account := range accounts {
		for _, token := range tokens {
			t.Run(name+"/"+token, func(t *testing.T) {
				got := make(http.Header, 4)
				want := make(http.Header, 4)
				// Every call site strips inbound auth before calling the helper;
				// seed the same stale headers so the parity check also covers the
				// "no stale credential survives" property.
				for _, k := range []string{"Authorization", "x-api-key", "x-goog-api-key", "Cookie"} {
					got.Set(k, "stale-inbound")
					want.Set(k, "stale-inbound")
				}

				setAnthropicAPIKeyAuthHeader(got, account, token, "")
				legacySetAnthropicAPIKeyAuthHeader(want, account, token)

				if !reflect.DeepEqual(got, want) {
					t.Fatalf("adapter output diverged from legacy:\n got: %v\nwant: %v", got, want)
				}
			})
		}
	}
}

func TestCopyUpstreamAuthHeadersIgnoresNonAuthHeaders(t *testing.T) {
	src := make(http.Header, 4)
	src.Set("authorization", "Bearer tok")
	// The Claude adapter also emits these; the bridge must not copy them, because
	// the gateway sets anthropic-version conditionally to preserve client values.
	src.Set("anthropic-version", "2023-06-01")
	src.Set("accept", "application/json")

	dst := make(http.Header, 2)
	copyUpstreamAuthHeaders(dst, src)

	if got := dst.Get("authorization"); got != "Bearer tok" {
		t.Fatalf("authorization = %q", got)
	}
	if got := dst.Get("anthropic-version"); got != "" {
		t.Fatalf("anthropic-version must not be copied by the auth bridge, got %q", got)
	}
	if got := dst.Get("accept"); got != "" {
		t.Fatalf("accept must not be copied by the auth bridge, got %q", got)
	}
}
