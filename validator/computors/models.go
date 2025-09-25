package computors

import (
	"encoding/hex"
	"fmt"
	"github.com/qubic/go-archiver-v2/protobuf"
	"github.com/qubic/go-node-connector/types"
)

const (
	SignatureSize     = 64
	NumberOfComputors = 676
)

type Computors struct {
	Epoch      uint16
	TickNumber uint32
	PubKeys    [NumberOfComputors][32]byte
	Signature  [SignatureSize]byte
}

func qubicToProto(computorsList []*Computors) (*protobuf.ComputorsList, error) {
	var compLists = make([]*protobuf.Computors, 0, len(computorsList))
	for _, computors := range computorsList {
		identities, err := pubKeysToIdentities(computors.PubKeys)
		if err != nil {
			return nil, fmt.Errorf("converting public keys to identities: %w", err)
		}
		compLists = append(compLists, &protobuf.Computors{
			Epoch:        uint32(computors.Epoch),
			TickNumber:   computors.TickNumber,
			Identities:   identities,
			SignatureHex: hex.EncodeToString(computors.Signature[:]),
		})
	}

	return &protobuf.ComputorsList{
		Computors: compLists,
	}, nil
}

func protoToQubic(computorsList *protobuf.ComputorsList) ([]*Computors, error) {
	var convertedLists = make([]*Computors, 0, len(computorsList.Computors))

	for _, computors := range computorsList.GetComputors() {
		pubKeys, err := identitiesToPubKeys(computors.Identities)
		if err != nil {
			return nil, fmt.Errorf("getting public keys: %w", err)
		}

		var sig [64]byte
		sigBytes, err := hex.DecodeString(computors.SignatureHex)
		if err != nil {
			return nil, fmt.Errorf("decoding hex signature: %w", err)
		}
		copy(sig[:], sigBytes)

		convertedLists = append(convertedLists, &Computors{
			Epoch:      uint16(computors.Epoch),
			TickNumber: computors.TickNumber,
			PubKeys:    pubKeys,
			Signature:  sig,
		})
	}

	return convertedLists, nil
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
