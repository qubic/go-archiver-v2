package quorum

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-archiver-v2/utils"
	"github.com/qubic/go-node-connector/types"
	"time"
)

func qubicToProto(votes types.QuorumVotes) *protobuf.QuorumTickData {
	firstQuorumTickData := votes[0]
	protoQuorumTickData := protobuf.QuorumTickData{
		QuorumTickStructure:   qubicTickStructureToProto(firstQuorumTickData),
		QuorumDiffPerComputor: make(map[uint32]*protobuf.QuorumDiff),
	}

	for _, quorumTickData := range votes {
		protoQuorumTickData.QuorumDiffPerComputor[uint32(quorumTickData.ComputorIndex)] = qubicDiffToProto(quorumTickData)
	}

	return &protoQuorumTickData
}

func qubicTickStructureToProto(tickVote types.QuorumTickVote) *protobuf.QuorumTickStructure {
	date := time.Date(2000+int(tickVote.Year), time.Month(tickVote.Month), int(tickVote.Day), int(tickVote.Hour), int(tickVote.Minute), int(tickVote.Second), 0, time.UTC)
	timestamp := date.UnixMilli() + int64(tickVote.Millisecond)
	protoQuorumTickStructure := protobuf.QuorumTickStructure{
		Epoch:      uint32(tickVote.Epoch),
		TickNumber: tickVote.Tick,
		Timestamp:  uint64(timestamp),

		PrevResourceTestingDigestHex: convertUint32ToHex(tickVote.PreviousResourceTestingDigest),
		PrevTransactionBodyHex:       convertUint32ToHex(tickVote.PreviousTransactionBodyDigest),

		PrevSpectrumDigestHex: hex.EncodeToString(tickVote.PreviousSpectrumDigest[:]),
		PrevUniverseDigestHex: hex.EncodeToString(tickVote.PreviousUniverseDigest[:]),
		PrevComputerDigestHex: hex.EncodeToString(tickVote.PreviousComputerDigest[:]),
		TxDigestHex:           hex.EncodeToString(tickVote.TxDigest[:]),
	}

	return &protoQuorumTickStructure
}

func qubicDiffToProto(tickVote types.QuorumTickVote) *protobuf.QuorumDiff {
	protoQuorumDiff := protobuf.QuorumDiff{
		SaltedResourceTestingDigestHex: convertUint32ToHex(tickVote.SaltedResourceTestingDigest),
		SaltedTransactionBodyHex:       convertUint32ToHex(tickVote.SaltedTransactionBodyDigest),

		SaltedSpectrumDigestHex:     hex.EncodeToString(tickVote.SaltedSpectrumDigest[:]),
		SaltedUniverseDigestHex:     hex.EncodeToString(tickVote.SaltedUniverseDigest[:]),
		SaltedComputerDigestHex:     hex.EncodeToString(tickVote.SaltedComputerDigest[:]),
		ExpectedNextTickTxDigestHex: hex.EncodeToString(tickVote.ExpectedNextTickTxDigest[:]),
		SignatureHex:                hex.EncodeToString(tickVote.Signature[:]),
	}
	return &protoQuorumDiff
}

func convertUint32ToHex(value uint32) string {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, value)
	return hex.EncodeToString(b)
}

func qubicToProtoStored(votes types.QuorumVotes) *protobuf.QuorumTickDataStored {
	firstQuorumTickData := votes[0]
	protoQuorumTickData := protobuf.QuorumTickDataStored{
		QuorumTickStructure:   qubicTickStructureToProto(firstQuorumTickData),
		QuorumDiffPerComputor: make(map[uint32]*protobuf.QuorumDiffStored),
	}

	for _, quorumTickData := range votes {
		protoQuorumTickData.QuorumDiffPerComputor[uint32(quorumTickData.ComputorIndex)] = qubicDiffToProtoStored(quorumTickData)
	}

	return &protoQuorumTickData
}

func qubicDiffToProtoStored(tickVote types.QuorumTickVote) *protobuf.QuorumDiffStored {
	protoQuorumDiff := protobuf.QuorumDiffStored{
		ExpectedNextTickTxDigestHex: hex.EncodeToString(tickVote.ExpectedNextTickTxDigest[:]),
		SignatureHex:                hex.EncodeToString(tickVote.Signature[:]),
	}
	return &protoQuorumDiff
}

// ReconstructQuorumData Used for querying from rpc server
func ReconstructQuorumData(currentTickQuorumData, nextTickQuorumData *protobuf.QuorumTickDataStored, computors *protobuf.Computors) (*protobuf.QuorumTickData, error) {

	reconstructedQuorumData := protobuf.QuorumTickData{
		QuorumTickStructure:   currentTickQuorumData.QuorumTickStructure,
		QuorumDiffPerComputor: make(map[uint32]*protobuf.QuorumDiff),
	}

	epoch := currentTickQuorumData.QuorumTickStructure.Epoch

	spectrumDigest, err := hex.DecodeString(nextTickQuorumData.QuorumTickStructure.PrevSpectrumDigestHex)
	if err != nil {
		return nil, fmt.Errorf("decoding prev_spectrum_digest_hex: %v", err)
	}
	universeDigest, err := hex.DecodeString(nextTickQuorumData.QuorumTickStructure.PrevUniverseDigestHex)
	if err != nil {
		return nil, fmt.Errorf("decoding prev_universe_digest_hex: %v", err)
	}
	computerDigest, err := hex.DecodeString(nextTickQuorumData.QuorumTickStructure.PrevComputerDigestHex)
	if err != nil {
		return nil, fmt.Errorf("decoding prev_computer_digest_hex: %v", err)
	}

	resourceDigest, err := hex.DecodeString(nextTickQuorumData.QuorumTickStructure.PrevResourceTestingDigestHex)
	if err != nil {
		return nil, fmt.Errorf("decoding prev_resource_testing_digest_hex: %v", err)
	}

	transactionBodyDigest, err := hex.DecodeString(nextTickQuorumData.QuorumTickStructure.PrevTransactionBodyHex)
	if err != nil {
		return nil, fmt.Errorf("decoding prev_transaction_body_hex: %v", err)
	}

	for id, voteDiff := range currentTickQuorumData.QuorumDiffPerComputor {

		identity := types.Identity(computors.Identities[id])

		computorPublicKey, err := identity.ToPubKey(false)
		if err != nil {
			return nil, fmt.Errorf("obtaining public key for computor [%d] with identity [%s]: %w", id, identity, err)
		}

		var tmp [64]byte
		copy(tmp[:32], computorPublicKey[:])

		copy(tmp[32:], spectrumDigest[:])
		saltedSpectrumDigest, err := utils.K12Hash(tmp[:])
		if err != nil {
			return nil, fmt.Errorf("hashing salted spectrum digest: %w", err)
		}

		copy(tmp[32:], universeDigest[:])
		saltedUniverseDigest, err := utils.K12Hash(tmp[:])
		if err != nil {
			return nil, fmt.Errorf("hashing salted universe digest: %w", err)
		}

		copy(tmp[32:], computerDigest[:])
		saltedComputerDigest, err := utils.K12Hash(tmp[:])
		if err != nil {
			return nil, fmt.Errorf("hashing salted computer digest: %w", err)
		}

		newFormat := epoch >= 151

		// Establish length of returned digest based on format
		digestSize := 8
		if newFormat {
			digestSize = 4
		}

		// Create resource testing digest no matter what
		saltedResourceTestingDigestBytes, err := reconstructShortSaltedDigest(computorPublicKey[:], resourceDigest, newFormat)
		if err != nil {
			return nil, fmt.Errorf("reconstructing salted resource testing digest: %w", err)
		}
		saltedResourceTestingDigest := hex.EncodeToString(saltedResourceTestingDigestBytes[:digestSize])

		// Declare transaction body digest no matter what and populate only if new data format
		var saltedTransactionBodyDigest string
		if newFormat {
			saltedTransactionBodyDigestBytes, err := reconstructShortSaltedDigest(computorPublicKey[:], transactionBodyDigest, true)
			if err != nil {
				return nil, fmt.Errorf("reconstructing salted transaction body digest: %w", err)
			}
			saltedTransactionBodyDigest = hex.EncodeToString(saltedTransactionBodyDigestBytes[:digestSize])
		}

		reconstructedQuorumData.QuorumDiffPerComputor[id] = &protobuf.QuorumDiff{
			SaltedResourceTestingDigestHex: saltedResourceTestingDigest,
			SaltedTransactionBodyHex:       saltedTransactionBodyDigest,
			SaltedSpectrumDigestHex:        hex.EncodeToString(saltedSpectrumDigest[:]),
			SaltedUniverseDigestHex:        hex.EncodeToString(saltedUniverseDigest[:]),
			SaltedComputerDigestHex:        hex.EncodeToString(saltedComputerDigest[:]),
			ExpectedNextTickTxDigestHex:    voteDiff.ExpectedNextTickTxDigestHex,
			SignatureHex:                   voteDiff.SignatureHex,
		}
	}

	return &reconstructedQuorumData, nil
}

func reconstructShortSaltedDigest(computorPubkey, prevDigest []byte, newFormat bool) ([32]byte, error) {

	var buf [40]byte
	copy(buf[:32], computorPubkey)
	copy(buf[32:], prevDigest)

	buf2 := buf[:]
	if newFormat {
		buf2 = buf2[:36]
	}

	saltedDigest, err := utils.K12Hash(buf2)
	if err != nil {
		return [32]byte{}, fmt.Errorf("hashing salted digest: %w", err)
	}
	return saltedDigest, nil
}
