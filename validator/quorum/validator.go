package quorum

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/qubic/go-archiver-v2/db"
	"github.com/qubic/go-archiver-v2/network"
	"github.com/qubic/go-archiver-v2/utils"
	"github.com/qubic/go-archiver-v2/validator/computors"
	"github.com/qubic/go-node-connector/types"
	"golang.org/x/sync/errgroup"
	"log"
	"slices"
)

// Validate validates the quorum votes and if success returns the aligned votes back
func Validate(ctx context.Context, store *db.PebbleStore, client network.QubicClient, quorumVotes types.QuorumVotes, computors computors.Computors, epoch uint16) (types.QuorumVotes, error) {
	targetTickVoteSignature, err := store.GetTargetTickVoteSignature(uint32(epoch))
	if err != nil {
		if !errors.Is(err, db.ErrNotFound) {
			return types.QuorumVotes{}, fmt.Errorf("getting target tick vote signature: %w", err)
		}
		systemInfo, err := client.GetSystemInfo(ctx)
		if err != nil {
			return types.QuorumVotes{}, fmt.Errorf("getting system info: %w", err)
		}
		err = store.SetTargetTickVoteSignature(uint32(systemInfo.Epoch), systemInfo.TargetTickVoteSignature)
		if err != nil {
			return types.QuorumVotes{}, fmt.Errorf("saving target tick vote signature: %w", err)
		}
		targetTickVoteSignature = systemInfo.TargetTickVoteSignature
	}
	return validateVotes(ctx, quorumVotes, computors, targetTickVoteSignature)

}

func validateVotes(ctx context.Context, quorumVotes types.QuorumVotes, computors computors.Computors, targetTickVoteSignature uint32) (types.QuorumVotes, error) {
	if len(quorumVotes) < types.MinimumQuorumVotes {
		return nil, errors.New("not enough quorum votes")
	}

	alignedVotes, err := getAlignedVotes(quorumVotes)
	if err != nil {
		return nil, fmt.Errorf("getting aligned votes: %w", err)
	}

	if len(alignedVotes) < types.MinimumQuorumVotes {
		return nil, fmt.Errorf("not enough aligned votes [%d]", len(alignedVotes))
	}

	err = quorumTickSigVerify(ctx, alignedVotes, computors, targetTickVoteSignature)
	if err != nil {
		return nil, fmt.Errorf("verifying tick signature: %w", err)
	}
	return alignedVotes, nil
}

type vote struct {
	Epoch                         uint16
	Tick                          uint32
	Millisecond                   uint16
	Second                        uint8
	Minute                        uint8
	Hour                          uint8
	Day                           uint8
	Month                         uint8
	Year                          uint8
	PreviousResourceTestingDigest uint32
	PreviousSpectrumDigest        [32]byte
	PreviousUniverseDigest        [32]byte
	PreviousComputerDigest        [32]byte
	TxDigest                      [32]byte
}

func getAlignedVotes(quorumVotes types.QuorumVotes) (types.QuorumVotes, error) {
	votesHeatMap := make(map[[32]byte]types.QuorumVotes)
	for i, qv := range quorumVotes {
		v := vote{
			Epoch:                         qv.Epoch,
			Tick:                          qv.Tick,
			Millisecond:                   qv.Millisecond,
			Second:                        qv.Second,
			Minute:                        qv.Minute,
			Hour:                          qv.Hour,
			Day:                           qv.Day,
			Month:                         qv.Month,
			Year:                          qv.Year,
			PreviousResourceTestingDigest: qv.PreviousResourceTestingDigest,
			PreviousSpectrumDigest:        qv.PreviousSpectrumDigest,
			PreviousUniverseDigest:        qv.PreviousUniverseDigest,
			PreviousComputerDigest:        qv.PreviousComputerDigest,
			TxDigest:                      qv.TxDigest,
		}
		digest, err := v.digest()
		if err != nil {
			return nil, fmt.Errorf("creating vote digest [%d]: %w", i, err)
		}
		if votes, ok := votesHeatMap[digest]; !ok {
			votesHeatMap[digest] = types.QuorumVotes{qv}
		} else {
			votesHeatMap[digest] = append(votes, qv)
		}
	}

	var alignedVotes types.QuorumVotes
	for _, votes := range votesHeatMap {
		if len(votes) > len(alignedVotes) {
			alignedVotes = votes
		}
	}

	return alignedVotes, nil
}

func (v *vote) digest() ([32]byte, error) {
	b, err := utils.BinarySerialize(v)
	if err != nil {
		return [32]byte{}, fmt.Errorf("serializing vote: %w", err)
	}

	digest, err := utils.K12Hash(b)
	if err != nil {
		return [32]byte{}, fmt.Errorf("hashing vote: %w", err)
	}

	return digest, nil
}

func quorumTickSigVerify(ctx context.Context, quorumVotes types.QuorumVotes, computors computors.Computors, targetTickVoteSignature uint32) error {
	var errorGroup errgroup.Group
	verifyChannel := make(chan int, len(quorumVotes)) // second argument is buffer capacity
	defer close(verifyChannel)

	for _, quorumTickData := range quorumVotes {
		errorGroup.Go(func() error {
			success, err := checkSignature(ctx, quorumTickData, computors, targetTickVoteSignature)
			if success > 0 {
				verifyChannel <- success
			}
			return err
		})
	}

	err := errorGroup.Wait()
	if err != nil {
		return fmt.Errorf("checking vote signatures: %w", err)
	}

	var successVotes = len(verifyChannel)
	if successVotes < types.MinimumQuorumVotes {
		return fmt.Errorf("not enough verified quorum votes: %d", successVotes)
	}
	return nil
}

func checkSignature(ctx context.Context, quorumTickData types.QuorumTickVote, computors computors.Computors, targetTickVoteSignature uint32) (int, error) {
	digest, err := getDigestFromQuorumTickData(quorumTickData)
	if err != nil {
		return 0, fmt.Errorf("creating digest from quorum tick data: %w", err)
	}
	computorPubKey := computors.PubKeys[quorumTickData.ComputorIndex]
	if err := verifyTickVoteSignature(ctx, utils.SchnorrqVerify, computorPubKey, digest, quorumTickData.Signature, targetTickVoteSignature); err != nil {
		// the following is only additional debug information to identify the failing computor
		var badComputor types.Identity
		badComputor, ce := badComputor.FromPubKey(computorPubKey, false)
		if ce != nil { // we don't fail in this case, we simply don't have the identity
			log.Printf("[ERROR] trying to convert computor public key to identity: %v", ce)
		} else {
			log.Printf("[ERROR] failing tick vote signature computor identity (#%d): %s", quorumTickData.ComputorIndex, string(badComputor))
		}
		return 0, fmt.Errorf("tick vote signature verification failed for computor index [%d]: %w", quorumTickData.ComputorIndex, err)
	}
	return 1, nil // success
}

func verifyTickVoteSignature(ctx context.Context, sigVerifierFunc utils.SigVerifierFunc, computorPubKey, digest [32]byte, signature [64]byte, targetTickVoteSignature uint32) error {
	invertedSignatureSection := swapBytes(signature[:4])
	score := binary.LittleEndian.Uint32(invertedSignatureSection)

	if score > targetTickVoteSignature {
		return errors.New("vote signature score over target tick vote signature")
	}

	return sigVerifierFunc(ctx, computorPubKey, digest, signature)
}

func swapBytes(input []byte) []byte {
	output := make([]byte, len(input))
	copy(output, input)
	slices.Reverse(output)

	return output
}

func getDigestFromQuorumTickData(data types.QuorumTickVote) ([32]byte, error) {
	// xor computor index with 8
	data.ComputorIndex ^= 3

	sData, err := utils.BinarySerialize(data)
	if err != nil {
		return [32]byte{}, fmt.Errorf("serializing quorum tick vote: %w", err)
	}

	tickData := sData[:len(sData)-64]
	digest, err := utils.K12Hash(tickData)
	if err != nil {
		return [32]byte{}, fmt.Errorf("hashing quorum tick vote: %w", err)
	}

	return digest, nil
}

func Store(ctx context.Context, store *db.PebbleStore, tickNumber uint32, quorumVotes types.QuorumVotes) error {
	protoModel := qubicToProtoStored(quorumVotes)

	err := store.SetQuorumTickData(ctx, tickNumber, protoModel)
	if err != nil {
		return fmt.Errorf("saving quorum tick votes data: %w", err)
	}

	fullProtoModel := qubicToProto(quorumVotes)

	err = store.SetQuorumDataForCurrentEpochInterval(fullProtoModel.QuorumTickStructure.Epoch, fullProtoModel)
	if err != nil {
		return fmt.Errorf("setting last quorum tick votes data: %w", err)
	}

	return nil
}
