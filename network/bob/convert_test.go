package bob

import (
	"testing"

	"github.com/qubic/go-node-connector/types"
	"github.com/stretchr/testify/require"
)

func TestQubicHashToBytes32_ZeroHash(t *testing.T) {
	// The all-A identity maps to all-zero bytes
	result, err := qubicHashToBytes32("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	require.NoError(t, err)
	require.Equal(t, [32]byte{}, result)
}

func TestQubicHashToBytes32_Empty(t *testing.T) {
	result, err := qubicHashToBytes32("")
	require.NoError(t, err)
	require.Equal(t, [32]byte{}, result)
}

func TestIdentityToBytes32_Empty(t *testing.T) {
	result, err := identityToBytes32("")
	require.NoError(t, err)
	require.Equal(t, [32]byte{}, result)
}

func TestIdentityToBytes32_Roundtrip(t *testing.T) {
	// Use a known identity and verify roundtrip
	identity := "BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAARMID"
	pubKey, err := identityToBytes32(identity)
	require.NoError(t, err)

	// Convert back
	var id types.Identity
	id, err = id.FromPubKey(pubKey, false)
	require.NoError(t, err)
	require.Equal(t, identity, string(id))
}

func TestHexToBytes64(t *testing.T) {
	hexStr := "0102030405060708091011121314151617181920212223242526272829303132" +
		"3334353637383940414243444546474849505152535455565758596061626364"
	result, err := hexToBytes64(hexStr)
	require.NoError(t, err)
	require.Equal(t, byte(0x01), result[0])
	require.Equal(t, byte(0x64), result[63])
}

func TestHexToBytes64_Empty(t *testing.T) {
	result, err := hexToBytes64("")
	require.NoError(t, err)
	require.Equal(t, [64]byte{}, result)
}

func TestHexToBytes64_WrongLength(t *testing.T) {
	_, err := hexToBytes64("0102")
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected 64 bytes")
}

func TestConvertVote(t *testing.T) {
	// Create a bob vote with zero-value digests (all-A qubic hash = zero bytes)
	zeroHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	v := bobVote{
		ComputorIndex:                     42,
		Epoch:                             150,
		Tick:                              12500000,
		Millisecond:                       500,
		Second:                            30,
		Minute:                            15,
		Hour:                              12,
		Day:                               28,
		Month:                             2,
		Year:                              25,
		PrevResourceTestingDigest:         1234,
		SaltedResourceTestingDigest:       5678,
		PrevTransactionBodyDigest:         9012,
		SaltedTransactionBodyDigest:       3456,
		PrevSpectrumDigest:                zeroHash,
		PrevUniverseDigest:                zeroHash,
		PrevComputerDigest:                zeroHash,
		SaltedSpectrumDigest:              zeroHash,
		SaltedUniverseDigest:              zeroHash,
		SaltedComputerDigest:              zeroHash,
		TransactionDigest:                 zeroHash,
		ExpectedNextTickTransactionDigest: zeroHash,
		Signature:                         "0000000000000000000000000000000000000000000000000000000000000000" + "0000000000000000000000000000000000000000000000000000000000000000",
	}

	result, err := convertVote(v)
	require.NoError(t, err)
	require.Equal(t, uint16(42), result.ComputorIndex)
	require.Equal(t, uint16(150), result.Epoch)
	require.Equal(t, uint32(12500000), result.Tick)
	require.Equal(t, uint16(500), result.Millisecond)
	require.Equal(t, uint32(1234), result.PreviousResourceTestingDigest)
	require.Equal(t, uint32(5678), result.SaltedResourceTestingDigest)
	require.Equal(t, uint32(9012), result.PreviousTransactionBodyDigest)
	require.Equal(t, uint32(3456), result.SaltedTransactionBodyDigest)
	require.Equal(t, [32]byte{}, result.TxDigest)
	require.Equal(t, [64]byte{}, result.Signature)
}

func TestConvertTickData(t *testing.T) {
	zeroHash := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	td := bobTickData{
		ComputorIndex:      5,
		Epoch:              150,
		Tick:               12500000,
		Millisecond:        100,
		Second:             45,
		Minute:             30,
		Hour:               10,
		Day:                15,
		Month:              3,
		Year:               25,
		Timelock:           zeroHash,
		TransactionDigests: []string{zeroHash},
		ContractFees:       jsonFees{100, 200},
		Signature:          "0000000000000000000000000000000000000000000000000000000000000000" + "0000000000000000000000000000000000000000000000000000000000000000",
	}

	result, err := convertTickData(td)
	require.NoError(t, err)
	require.Equal(t, uint16(5), result.ComputorIndex)
	require.Equal(t, uint16(150), result.Epoch)
	require.Equal(t, uint32(12500000), result.Tick)
	require.Equal(t, [32]byte{}, result.Timelock)
	require.Equal(t, [32]byte{}, result.TransactionDigests[0])
	require.Equal(t, int64(100), result.ContractFees[0])
	require.Equal(t, int64(200), result.ContractFees[1])
}

func TestConvertComputors(t *testing.T) {
	identities := []string{
		"BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAARMID",
		"BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAARMID",
	}

	result, err := convertComputors(identities, 150)
	require.NoError(t, err)
	require.Equal(t, uint16(150), result.Epoch)
	// Both identities map to same pubkey
	require.Equal(t, result.PubKeys[0], result.PubKeys[1])
	// Remaining should be zero
	require.Equal(t, [32]byte{}, result.PubKeys[2])
	// Signature is zero (bob doesn't provide it)
	require.Equal(t, [64]byte{}, result.Signature)
}

func TestConvertRPCTransaction(t *testing.T) {
	tx := bobRPCTransaction{
		Hash:      "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh",
		From:      "BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAARMID",
		To:        "BAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAARMID",
		Amount:    1000000,
		InputType: 0,
		InputSize: 0,
		InputData: "",
		Signature: "0000000000000000000000000000000000000000000000000000000000000000" + "0000000000000000000000000000000000000000000000000000000000000000",
	}

	result, err := convertRPCTransaction(tx)
	require.NoError(t, err)
	require.Equal(t, int64(1000000), result.Amount)
	require.Equal(t, uint16(0), result.InputType)
	require.Equal(t, uint16(0), result.InputSize)
	require.Nil(t, result.Input)
	require.Equal(t, [64]byte{}, result.Signature)
}

func TestJsonFees_UnmarshalJSON_Array(t *testing.T) {
	var fees jsonFees
	err := fees.UnmarshalJSON([]byte("[100, 200, 300]"))
	require.NoError(t, err)
	require.Equal(t, jsonFees{100, 200, 300}, fees)
}

func TestJsonFees_UnmarshalJSON_Zero(t *testing.T) {
	var fees jsonFees
	err := fees.UnmarshalJSON([]byte("0"))
	require.NoError(t, err)
	require.Nil(t, fees)
}
