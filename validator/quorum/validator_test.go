package quorum

import (
	"context"
	"testing"

	"github.com/qubic/go-archiver-v2/validator/computors"
	"github.com/qubic/go-node-connector/types"
	"github.com/stretchr/testify/require"
)

// Sufficient quorum votes.
// Insufficient quorum votes.
// Mismatched votes in various fields.
func TestValidateVotes(t *testing.T) {
	originalData := types.QuorumVotes{
		types.QuorumTickVote{
			ComputorIndex:                 1,
			Epoch:                         1,
			Tick:                          100,
			Millisecond:                   500,
			Second:                        30,
			Minute:                        15,
			Hour:                          12,
			Day:                           28,
			Month:                         2,
			Year:                          20,
			PreviousResourceTestingDigest: 1234567890,
			SaltedResourceTestingDigest:   876543210,
			PreviousSpectrumDigest:        nonEmptyDigest(1),
			PreviousUniverseDigest:        nonEmptyDigest(2),
			PreviousComputerDigest:        nonEmptyDigest(3),
			SaltedSpectrumDigest:          nonEmptyDigest(4),
			SaltedUniverseDigest:          nonEmptyDigest(5),
			SaltedComputerDigest:          nonEmptyDigest(6),
			TxDigest:                      nonEmptyDigest(7),
			ExpectedNextTickTxDigest:      nonEmptyDigest(8),
			Signature:                     [64]byte{},
		},
		// Duplicate the first entry for a valid comparison base
		{
			ComputorIndex:                 2,
			Epoch:                         1,
			Tick:                          100,
			Millisecond:                   500,
			Second:                        30,
			Minute:                        15,
			Hour:                          12,
			Day:                           28,
			Month:                         2,
			Year:                          20,
			PreviousResourceTestingDigest: 1234567890,
			SaltedResourceTestingDigest:   3543210321,
			PreviousSpectrumDigest:        nonEmptyDigest(1),
			PreviousUniverseDigest:        nonEmptyDigest(2),
			PreviousComputerDigest:        nonEmptyDigest(3),
			SaltedSpectrumDigest:          nonEmptyDigest(4),
			SaltedUniverseDigest:          nonEmptyDigest(5),
			SaltedComputerDigest:          nonEmptyDigest(6),
			TxDigest:                      nonEmptyDigest(7),
			ExpectedNextTickTxDigest:      nonEmptyDigest(8),
			Signature:                     [64]byte{},
		},
	}

	_, err := validateVotes(context.Background(), originalData, computors.Computors{}, 0)
	require.ErrorContains(t, err, "not enough quorum votes")

	cases := []struct {
		name          string
		modify        func(votes *types.QuorumVotes)
		expectedVotes int
		expectError   bool
	}{
		{
			name:          "valid data",
			modify:        func(votes *types.QuorumVotes) {},
			expectedVotes: 2,
		},
		// Test cases for mismatches in each field
		{
			name: "mismatched Second",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].Second += 1
				*votes = valueVotes
			},
			expectedVotes: 1,
		},
		{
			name: "mismatched Minute",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].Minute += 1
				*votes = valueVotes
			},
			expectedVotes: 1,
		},
		{
			name: "mismatched Hour",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].Hour += 1
				*votes = valueVotes
			},
			expectedVotes: 1,
		},
		{
			name: "mismatched Day",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].Day += 1
				*votes = valueVotes
			},
			expectedVotes: 1,
		},
		{
			name: "mismatched Month",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].Month += 1
				*votes = valueVotes
			},
			expectedVotes: 1,
		},
		{
			name: "mismatched Year",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].Year += 1
				*votes = valueVotes
			},
			expectedVotes: 1,
		},
		{
			name: "mismatched PreviousResourceTestingDigest",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].PreviousResourceTestingDigest += 1
				*votes = valueVotes
			},
			expectedVotes: 1,
		},
		{
			name: "mismatched PreviousSpectrumDigest",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].PreviousSpectrumDigest = nonEmptyDigest(2)
				*votes = valueVotes
			},
			expectedVotes: 1,
		},
		{
			name: "mismatched PreviousUniverseDigest",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].PreviousUniverseDigest[0] += 1
				*votes = valueVotes
			},
			expectedVotes: 1,
		},
		{
			name: "mismatched PreviousComputerDigest",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].PreviousComputerDigest[0] += 1
				*votes = valueVotes
			},
			expectedVotes: 1,
		},
		{
			name: "mismatched TxDigest",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].TxDigest[0] += 1
				*votes = valueVotes
			},
			expectedVotes: 1,
		},
		{
			name: "mismatched SaltedSpectrumDigest",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].SaltedSpectrumDigest[0] += 1
				*votes = valueVotes
			},
			expectedVotes: 2,
		},
		{
			name: "mismatched SaltedUniverseDigest",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].SaltedUniverseDigest[0] += 1
				*votes = valueVotes
			},
			expectedVotes: 2,
		},
		{
			name: "mismatched SaltedComputerDigest",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].SaltedComputerDigest[0] += 1
				*votes = valueVotes
			},
			expectedVotes: 2,
		},
		{
			name: "mismatched SaltedResourceTestingDigest",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].SaltedResourceTestingDigest += 1
				*votes = valueVotes
			},
			expectedVotes: 2,
		},
		{
			name: "mismatched ExpectedNextTickTxDigest",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].ExpectedNextTickTxDigest[0] += 1
				*votes = valueVotes
			},
			expectedVotes: 2,
		},
		{
			name: "mismatched Signature",
			modify: func(votes *types.QuorumVotes) {
				valueVotes := *votes
				valueVotes[1].Signature[0] += 1
				*votes = valueVotes
			},
			expectedVotes: 2,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Make a deep copy of the original data to avoid mutation between tests
			dataCopy := deepCopy(originalData)
			tc.modify(&dataCopy)

			alignedVotes, err := getAlignedVotes(dataCopy)
			require.NoError(t, err)
			require.Equal(t, len(alignedVotes), tc.expectedVotes)
		})
	}
}

func deepCopy(votes types.QuorumVotes) types.QuorumVotes {
	cp := make(types.QuorumVotes, len(votes))

	for i, qv := range votes {
		cp[i] = qv // Assuming QuorumTickData contains only value types; otherwise, further deep copy needed.
		// For [32]byte fields, direct assignment here is fine since arrays (unlike slices) are value types and copied.
	}

	return cp
}

// buildAlignedVotes returns count votes that share every alignment-relevant field
// (so getAlignedVotes returns all of them as one bucket). ComputorIndex differs per vote
// but is not part of the alignment digest, so it does not affect bucketing.
func buildAlignedVotes(count int, txDigest [32]byte) types.QuorumVotes {
	votes := make(types.QuorumVotes, count)
	for i := 0; i < count; i++ {
		votes[i] = types.QuorumTickVote{
			ComputorIndex:                 uint16(i),
			Epoch:                         1,
			Tick:                          100,
			Millisecond:                   500,
			Second:                        30,
			Minute:                        15,
			Hour:                          12,
			Day:                           28,
			Month:                         2,
			Year:                          20,
			PreviousResourceTestingDigest: 1234567890,
			PreviousSpectrumDigest:        nonEmptyDigest(1),
			PreviousUniverseDigest:        nonEmptyDigest(2),
			PreviousComputerDigest:        nonEmptyDigest(3),
			TxDigest:                      txDigest,
		}
	}
	return votes
}

func TestRequiredQuorumVotes(t *testing.T) {
	// Empty ticks need only the relaxed threshold; non-empty ticks need full BFT quorum.
	require.Equal(t, EmptyTickMinVoteCount, requiredQuorumVotes(true))
	require.Equal(t, types.MinimumQuorumVotes, requiredQuorumVotes(false))
}

func TestValidateVotes_BelowFloor(t *testing.T) {
	// 225 votes is below the 226-vote floor and must be rejected before alignment runs.
	votes := buildAlignedVotes(EmptyTickMinVoteCount-1, [32]byte{})

	_, err := validateVotes(context.Background(), votes, computors.Computors{}, 0)
	require.ErrorContains(t, err, "not enough quorum votes")
}

func TestValidateVotes_EmptyTick_BelowAlignmentThreshold(t *testing.T) {
	// 226 empty-tick votes total but one is misaligned, so only 225 align — one below
	// the relaxed empty-tick threshold. Must be rejected at the post-alignment count gate.
	votes := buildAlignedVotes(EmptyTickMinVoteCount, [32]byte{})
	votes[0].Second += 1 // breaks alignment for this vote only

	_, err := validateVotes(context.Background(), votes, computors.Computors{}, 0)
	require.ErrorContains(t, err, "not enough aligned votes [225]")
	require.ErrorContains(t, err, "required [226]")
}

func TestValidateVotes_NonEmptyTick_StillRequiresFullQuorum(t *testing.T) {
	// 300 aligned votes for a non-empty tick must still be rejected at the count gate;
	// the relaxed empty-tick threshold must not bleed into the non-empty path.
	votes := buildAlignedVotes(300, nonEmptyDigest(7))

	_, err := validateVotes(context.Background(), votes, computors.Computors{}, 0)
	require.ErrorContains(t, err, "not enough aligned votes [300]")
	require.ErrorContains(t, err, "required [451]")
}

func TestIsEmptyTick(t *testing.T) {
	t.Run("zero TxDigest returns true", func(t *testing.T) {
		require.True(t, IsEmptyTick(types.QuorumVotes{{TxDigest: [32]byte{}}}))
	})
	t.Run("non-zero TxDigest returns false", func(t *testing.T) {
		require.False(t, IsEmptyTick(types.QuorumVotes{{TxDigest: nonEmptyDigest(1)}}))
	})
	t.Run("empty slice panics", func(t *testing.T) {
		require.Panics(t, func() { IsEmptyTick(types.QuorumVotes{}) })
	})
}

func TestByteSwap(t *testing.T) {

	testData := []struct {
		name     string
		data     []byte
		expected []byte
	}{
		{
			name:     "Test_ByteSwap1",
			data:     []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06},
			expected: []byte{0x06, 0x05, 0x04, 0x03, 0x02, 0x01},
		},
	}

	for _, testCase := range testData {
		t.Run(testCase.name, func(t *testing.T) {

			got := swapBytes(testCase.data)
			require.Equal(t, testCase.expected, got)
		})
	}

}
