package computors

import (
	"context"
	"fmt"
	"github.com/qubic/go-archiver/utils"
	"github.com/qubic/go-node-connector/types"
)

func Validate(ctx context.Context, computors types.Computors, arbitratorPubKey [32]byte) error {
	digest, err := getDigestFromComputors(computors)
	if err != nil {
		return fmt.Errorf("getting computors digest: %w", err)
	}

	err = utils.SchnorrqVerify(ctx, arbitratorPubKey, digest, computors.Signature)
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
