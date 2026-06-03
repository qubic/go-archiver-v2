package tx

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/qubic/go-node-connector/v2/types"
	"github.com/stretchr/testify/require"
)

// When the node returns more transactions than the tick data references, the
// extras (transactions whose digest is not in the tick data) must be silently
// dropped rather than aborting validation. The contract is that every tick-data
// digest is accounted for, not that every reported transaction is referenced.
func TestValidateTransactions_ExtraTransactionsAreSkipped(t *testing.T) {
	extraTx := types.Transaction{
		SourcePublicKey:      [32]byte{1},
		DestinationPublicKey: [32]byte{2},
		Amount:               42,
		Tick:                 20,
	}

	got, err := validateTransactions(context.Background(), []types.Transaction{extraTx}, map[string]struct{}{})
	require.NoError(t, err)
	require.Empty(t, got)
}

// Guards the invariant that survives the fix: a digest the tick data references
// but the node did not provide is still a fatal error.
func TestValidateTransactions_MissingDigestStillErrors(t *testing.T) {
	missing := [32]byte{0xab, 0xcd}
	digestsMap := map[string]struct{}{
		hex.EncodeToString(missing[:]): {},
	}

	got, err := validateTransactions(context.Background(), nil, digestsMap)
	require.Error(t, err)
	require.Nil(t, got)
}
