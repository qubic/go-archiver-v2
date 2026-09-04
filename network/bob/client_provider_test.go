package bob

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// nodesResponse builds a fetcher /status payload listing the given instance addresses.
func nodesResponse(addresses ...string) string {
	nodes := make([]string, 0, len(addresses))
	for _, a := range addresses {
		nodes = append(nodes, fmt.Sprintf(`{"address":%q}`, a))
	}
	return fmt.Sprintf(`{"max_tick":100,"number_of_configured_nodes":%d,"reliable_nodes":[%s]}`,
		len(addresses), strings.Join(nodes, ","))
}

// providerServing returns a provider wired to a fetcher that always reports the given
// instances. The update interval is irrelevant: these tests drive updateBobAddresses
// directly rather than starting the refresh loop.
func providerServing(t *testing.T, protocol, port string, addresses ...string) (*ClientProvider, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(nodesResponse(addresses...)))
	}))
	t.Cleanup(srv.Close)
	return NewProvider(srv.URL, protocol, port, time.Hour), srv
}

// A failed refresh must drop the instances it had rather than keep serving them. Bob
// instances are only safe to use while the fetcher vouches for them as reliable and in
// sync, so falling back to a stale list would hand out instances that may have drifted.
func TestClientProvider_updateBobAddresses_clearsInstancesOnFetcherError(t *testing.T) {
	tests := []struct {
		name       string
		failWith   http.HandlerFunc
		wantErrHas string
	}{
		{
			name: "non-200 response",
			failWith: func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "fetcher unwell", http.StatusInternalServerError)
			},
			wantErrHas: "response status not OK (500)",
		},
		{
			name: "malformed json",
			failWith: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(`{"reliable_nodes":[`))
			},
			wantErrHas: "unmarshalling response",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var failing atomic.Bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if failing.Load() {
					tc.failWith(w, r)
					return
				}
				_, _ = w.Write([]byte(nodesResponse("10.0.0.1", "10.0.0.2")))
			}))
			defer srv.Close()

			p := NewProvider(srv.URL, "http", "40420", time.Hour)

			require.NoError(t, p.updateBobAddresses())
			require.Len(t, p.bobAddresses, 2, "sanity: instances discovered before the fetcher fails")

			failing.Store(true)
			err := p.updateBobAddresses()
			require.ErrorContains(t, err, tc.wantErrHas)
			require.Empty(t, p.bobAddresses, "a failed refresh must clear the instances, not keep stale ones")

			_, err = p.GetClient()
			require.ErrorContains(t, err, "no available bob instances")
		})
	}
}

// Same requirement for the fetcher becoming unreachable, which is the shape an outage
// actually takes: the transport fails rather than the response being bad.
func TestClientProvider_updateBobAddresses_clearsInstancesWhenFetcherUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(nodesResponse("10.0.0.1")))
	}))
	p := NewProvider(srv.URL, "http", "40420", time.Hour)

	require.NoError(t, p.updateBobAddresses())
	require.Len(t, p.bobAddresses, 1)

	srv.Close()

	err := p.updateBobAddresses()
	require.ErrorContains(t, err, "performing request")
	require.Empty(t, p.bobAddresses)
}

// A fetcher that answers successfully with nothing reliable leaves us with no instances,
// which must surface as the same error rather than an empty or malformed client.
func TestClientProvider_updateBobAddresses_emptyReliableNodeList(t *testing.T) {
	p, _ := providerServing(t, "http", "40420")

	require.NoError(t, p.updateBobAddresses(), "an empty list is a valid fetcher answer, not an error")
	require.Empty(t, p.bobAddresses)

	_, err := p.GetClient()
	require.ErrorContains(t, err, "no available bob instances")
}

func TestClientProvider_GetClient_composesInstanceAddress(t *testing.T) {
	p, _ := providerServing(t, "https", "1234", "10.0.0.7")
	require.NoError(t, p.updateBobAddresses())

	client, err := p.GetClient()
	require.NoError(t, err)
	require.Equal(t, "https://10.0.0.7:1234", client.bobAddress)
	require.Same(t, p.httpClient, client.httpClient, "clients share the provider's connection pool")
}

// Guards against the address slice being padded with empty entries, which would compose
// hostless addresses like "http://:40420" and silently send requests nowhere. Also
// confirms the random pick reaches every discovered instance rather than sticking to one.
func TestClientProvider_GetClient_onlyReturnsDiscoveredInstances(t *testing.T) {
	p, _ := providerServing(t, "http", "40420", "10.0.0.1", "10.0.0.2", "10.0.0.3")
	require.NoError(t, p.updateBobAddresses())

	want := map[string]bool{
		"http://10.0.0.1:40420": true,
		"http://10.0.0.2:40420": true,
		"http://10.0.0.3:40420": true,
	}
	seen := make(map[string]int, len(want))
	for i := 0; i < 300; i++ {
		client, err := p.GetClient()
		require.NoError(t, err)
		require.True(t, want[client.bobAddress], "handed out an instance that was never discovered: %q", client.bobAddress)
		seen[client.bobAddress]++
	}
	require.Len(t, seen, len(want), "random pick should reach every discovered instance")
}

func TestClientProvider_GetClient_withoutAnyDiscovery(t *testing.T) {
	p := NewProvider("http://127.0.0.1:1/status", "http", "40420", time.Hour)

	client, err := p.GetClient()
	require.Nil(t, client)
	require.ErrorContains(t, err, "no available bob instances")
}
