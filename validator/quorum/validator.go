package quorum

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/db"
	"github.com/qubic/go-archiver/network"
	"github.com/qubic/go-archiver/utils"
	"github.com/qubic/go-node-connector/types"
	"log"
	"slices"
)

// Validate validates the quorum votes and if success returns the aligned votes back
func Validate(ctx context.Context, store *db.PebbleStore, client network.QubicClient, quorumVotes types.QuorumVotes, computors types.Computors, epoch uint16) (types.QuorumVotes, error) {
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

func validateVotes(ctx context.Context, quorumVotes types.QuorumVotes, computors types.Computors, targetTickVoteSignature uint32) (types.QuorumVotes, error) {
	if len(quorumVotes) < types.MinimumQuorumVotes {
		return nil, errors.New("not enough quorum votes")
	}

	log.Printf("Number of total votes: %d", len(quorumVotes))
	alignedVotes, err := getAlignedVotes(quorumVotes)
	if err != nil {
		return nil, fmt.Errorf("getting aligned votes: %w", err)
	}

	if len(alignedVotes) < types.MinimumQuorumVotes {
		return nil, fmt.Errorf("not enough aligned votes [%d]", len(alignedVotes))
	}

	log.Printf("Validating [%d] aligned quorum votes.", len(alignedVotes))
	err = quorumTickSigVerify(ctx, utils.SchnorrqVerify, alignedVotes, computors, targetTickVoteSignature)
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

func quorumTickSigVerify(ctx context.Context, sigVerifierFunc utils.SigVerifierFunc, quorumVotes types.QuorumVotes, computors types.Computors, targetTickVoteSignature uint32) error {
	var successVotes = 0
	failedIndexes := make([]uint16, 0, 0)
	failedIdentites := make([]string, 0, 0)
	//log.Printf("Proceed to validate total quorum votes: %d\n", len(quorumVotes))
	for _, quorumTickData := range quorumVotes {
		digest, err := getDigestFromQuorumTickData(quorumTickData)
		if err != nil {
			return fmt.Errorf("getting digest from quorum tick data: %w", err)
		}
		computorPubKey := computors.PubKeys[quorumTickData.ComputorIndex]
		if err := verifyTickVoteSignature(ctx, sigVerifierFunc, computorPubKey, digest, quorumTickData.Signature, targetTickVoteSignature); err != nil {
			//return fmt.Errorf("quorum tick signature verification failed for computor index %d: %w", quorumTickData.ComputorIndex, err)
			//log.Printf("Quorum tick signature verification failed for computor index: %d. Err: %s\n", quorumTickData.ComputorIndex, err.Error())
			failedIndexes = append(failedIndexes, quorumTickData.ComputorIndex)
			var badComputor types.Identity
			badComputor, err = badComputor.FromPubKey(computorPubKey, false)
			if err != nil {
				return fmt.Errorf("getting bad computor identity: %w", err)
			}
			failedIdentites = append(failedIdentites, string(badComputor))
			continue
		}
		successVotes += 1
		//log.Printf("Validated vote for computor index: %d. Vote number %d\n", quorumTickData.ComputorIndex, successVotes)
	}

	if successVotes < types.MinimumQuorumVotes {
		return errors.Errorf("not enough sucessful quorum votes: %d", successVotes)
	}
	log.Printf("Quorum votes: %d VALIDATED |  %d FAILED\n", successVotes, len(failedIndexes))

	//log.Printf("Validated total quorum votes: %d\n", successVotes)
	//log.Printf("Unvalidated total quorum votes: %d. List: %v, %v\n", len(failedIndexes), failedIndexes, failedIdentites)
	return nil
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
