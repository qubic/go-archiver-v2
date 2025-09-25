package computors

import (
	"encoding/hex"
	"fmt"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-node-connector/types"
)

func qubicToProto(computors types.Computors) (*protobuf.Computors, error) {
	identities, err := pubKeysToIdentities(computors.PubKeys)
	if err != nil {
		return nil, fmt.Errorf("getting identities: %w", err)
	}
	return &protobuf.Computors{
		Epoch:        uint32(computors.Epoch),
		Identities:   identities,
		SignatureHex: hex.EncodeToString(computors.Signature[:]),
	}, nil
}

func protoToQubic(computors *protobuf.Computors) (types.Computors, error) {
	pubKeys, err := identitiesToPubKeys(computors.Identities)
	if err != nil {
		return types.Computors{}, fmt.Errorf("getting public keys: %w", err)
	}

	var sig [64]byte
	sigBytes, err := hex.DecodeString(computors.SignatureHex)
	if err != nil {
		return types.Computors{}, fmt.Errorf("decoding hex signature: %w", err)
	}
	copy(sig[:], sigBytes)

	return types.Computors{
		Epoch:     uint16(int(computors.Epoch)),
		PubKeys:   pubKeys,
		Signature: sig,
	}, nil
}

func pubKeysToIdentities(pubKeys [types.NumberOfComputors][32]byte) ([]string, error) {
	identities := make([]string, 0)
	for _, identity := range pubKeys {
		if identity == [32]byte{} {
			continue
		}
		var id types.Identity
		id, err := id.FromPubKey(identity, false)
		if err != nil {
			return nil, fmt.Errorf("converting public key to identity [%s]: %w", hex.EncodeToString(identity[:]), err)
		}
		identities = append(identities, id.String())
	}
	return identities, nil
}

func identitiesToPubKeys(identities []string) ([types.NumberOfComputors][32]byte, error) {
	var pubKeys [types.NumberOfComputors][32]byte
	for i, identity := range identities {
		if len(identity) == 0 {
			continue
		}
		id := types.Identity(identity)
		pubKeyBytes, err := id.ToPubKey(false)
		if err != nil {
			return pubKeys, fmt.Errorf("converting identity [%s] to public key: %w", identity, err)
		}
		pubKeys[i] = pubKeyBytes
	}

	return pubKeys, nil
}
