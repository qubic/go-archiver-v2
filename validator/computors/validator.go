package computors

import (
	"context"
	"fmt"
	"github.com/qubic/go-archiver/utils"
	"github.com/qubic/go-node-connector/types"
	"github.com/qubic/go-schnorrq"
)

func Validate(ctx context.Context, computors types.Computors, arbitratorPubKey [32]byte) error {
	digest, err := getDigestFromComputors(computors)
	if err != nil {
		return fmt.Errorf("getting computors digest: %w", err)
	}

	err = schnorrqVerify(ctx, arbitratorPubKey, digest, computors.Signature)
	if err != nil {
		return fmt.Errorf("validating computor signatures: %w", err)
	}

	return nil
}

func getDigestFromComputors(data types.Computors) ([32]byte, error) {
	sData, err := utils.BinarySerialize(data)
	if err != nil {
		return [32]byte{}, fmt.Errorf("serializing computors: %w", err)
	}

	// remove signature from computors data
	computorsData := sData[:len(sData)-64]
	digest, err := utils.K12Hash(computorsData)
	if err != nil {
		return [32]byte{}, fmt.Errorf("hashing computors: %w", err)
	}

	return digest, nil
}

func schnorrqVerify(ctx context.Context, pubkey [32]byte, digest [32]byte, sig [64]byte) error {
	return schnorrq.Verify(pubkey, digest, sig)
}
