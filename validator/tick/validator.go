package tick

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/utils"
	"github.com/qubic/go-node-connector/types"
)

func Validate(ctx context.Context, data types.TickData, quorumTickVote types.QuorumTickVote, comps types.Computors) error {
	if data.Epoch == 0xffff {
		data.Epoch = 0
	}

	// empty tick with empty quorum tx digest means other verification is not needed
	if (data.IsEmpty()) && quorumTickVote.TxDigest == [32]byte{} {
		return nil
	}

	if data.Epoch == 0 {
		data.Epoch = quorumTickVote.Epoch
	}

	computorPubKey := comps.PubKeys[data.ComputorIndex]

	digest, err := getDigestFromTickData(data)
	if err != nil {
		return fmt.Errorf("getting partial tick data digest: %w", err)
	}

	// verify tick signature
	err = utils.SchnorrqVerify(ctx, computorPubKey, digest, data.Signature)
	if err != nil {
		return fmt.Errorf("verifying tick signature: %w", err)
	}

	fullDigest, err := getFullDigestFromTickData(data)
	if err != nil {
		return fmt.Errorf("getting full tick data digest: %w", err)
	}

	if fullDigest != quorumTickVote.TxDigest {
		return fmt.Errorf("quorum tx digest mismatch, full digest [%s], quorum tx digest [%s]",
			hex.EncodeToString(fullDigest[:]), hex.EncodeToString(quorumTickVote.TxDigest[:]))
	}

	return nil
}

func getDigestFromTickData(data types.TickData) ([32]byte, error) {
	// xor computorIndex with packet type
	// this is a security feature to avoid reusing the signature for another message type
	data.ComputorIndex ^= 8

	sData, err := utils.BinarySerialize(data)
	if err != nil {
		return [32]byte{}, fmt.Errorf("serializing tick data: %w", err)
	}

	tickData := sData[:len(sData)-64] // only part
	digest, err := utils.K12Hash(tickData)
	if err != nil {
		return [32]byte{}, fmt.Errorf("hashing tick data: %w", err)
	}

	return digest, nil
}

func getFullDigestFromTickData(data types.TickData) ([32]byte, error) {
	sData, err := utils.BinarySerialize(data)
	if err != nil {
		return [32]byte{}, fmt.Errorf("serializing tick data: %w", err)
	}

	tickData := sData[:]
	digest, err := utils.K12Hash(tickData)
	if err != nil {
		return [32]byte{}, fmt.Errorf("hashing tick data: %w", err)
	}

	return digest, nil
}

func Store(ctx context.Context, store *db.PebbleStore, tickNumber uint32, tickData types.TickData) error {
	protoTickData, err := qubicToProto(tickData)
	if err != nil {
		return fmt.Errorf("converting tick data to proto: %w", err)
	}

	err = store.SetTickData(ctx, tickNumber, protoTickData)
	if err != nil {
		return fmt.Errorf("saving tick data: %w", err)
	}
	return nil
}
