package quorum

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"github.com/qubic/go-archiver/db"
	"github.com/qubic/go-archiver/utils"
	qubic "github.com/qubic/go-node-connector"
	"github.com/qubic/go-node-connector/types"
	"github.com/qubic/go-schnorrq"
	"log"
	"slices"
)

// Validate validates the quorum votes and if success returns the aligned votes back
func Validate(ctx context.Context, store *db.PebbleStore, client *qubic.Client, quorumVotes types.QuorumVotes, computors types.Computors, epoch uint16) (types.QuorumVotes, error) {
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
	return validateVotes(ctx, schnorrqVerify, quorumVotes, computors, targetTickVoteSignature)

}

func validateVotes(ctx context.Context, sigVerifierFunc utils.SigVerifierFunc, quorumVotes types.QuorumVotes, computors types.Computors, targetTickVoteSignature uint32) (types.QuorumVotes, error) {
	if len(quorumVotes) < types.MinimumQuorumVotes {
		return nil, errors.New("not enough quorum votes")
	}

	log.Printf("Number of total votes: %d", len(quorumVotes))
	alignedVotes, err := getAlignedVotes(quorumVotes)
	if err != nil {
		return nil, errors.Wrap(err, "getting aligned votes")
	}

	if len(alignedVotes) < types.MinimumQuorumVotes {
		return nil, errors.Errorf("too few aligned votes: [%d]", alignedVotes)
	}

	log.Printf("Proceed to validate total quorum sigs: %d", len(alignedVotes))
	err = quorumTickSigVerify(ctx, sigVerifierFunc, alignedVotes, computors, targetTickVoteSignature)
	if err != nil {
		return nil, errors.Wrap(err, "quorum tick signature verification failed")
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
	for _, qv := range quorumVotes {
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
			return nil, errors.Wrap(err, "getting digest")
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
		return [32]byte{}, errors.Wrap(err, "serializing vote")
	}

	digest, err := utils.K12Hash(b)
	if err != nil {
		return [32]byte{}, errors.Wrap(err, "hashing vote")
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
			return errors.Wrap(err, "getting digest from tick data")
		}
		computorPubKey := computors.PubKeys[quorumTickData.ComputorIndex]
		if err := verifyTickVoteSignature(ctx, sigVerifierFunc, computorPubKey, digest, quorumTickData.Signature, targetTickVoteSignature); err != nil {
			//return errors.Wrapf(err, "quorum tick signature verification failed for computor index: %d", quorumTickData.ComputorIndex)
			//log.Printf("Quorum tick signature verification failed for computor index: %d. Err: %s\n", quorumTickData.ComputorIndex, err.Error())
			failedIndexes = append(failedIndexes, quorumTickData.ComputorIndex)
			var badComputor types.Identity
			badComputor, err = badComputor.FromPubKey(computorPubKey, false)
			if err != nil {
				return errors.Wrap(err, "getting bad computor")
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
		return [32]byte{}, errors.Wrap(err, "serializing data")
	}

	tickData := sData[:len(sData)-64]
	digest, err := utils.K12Hash(tickData)
	if err != nil {
		return [32]byte{}, errors.Wrap(err, "hashing tick data")
	}

	return digest, nil
}

func Store(ctx context.Context, store *db.PebbleStore, tickNumber uint32, quorumVotes types.QuorumVotes) error {
	protoModel := qubicToProtoStored(quorumVotes)

	err := store.SetQuorumTickData(ctx, tickNumber, protoModel)
	if err != nil {
		return errors.Wrap(err, "set quorum votes")
	}

	fullProtoModel := qubicToProto(quorumVotes)

	err = store.SetQuorumDataForCurrentEpochInterval(fullProtoModel.QuorumTickStructure.Epoch, fullProtoModel)
	if err != nil {
		return errors.Wrap(err, "setting last quorum tick data")
	}

	return nil
}

func schnorrqVerify(_ context.Context, pubkey [32]byte, digest [32]byte, sig [64]byte) error {
	return schnorrq.Verify(pubkey, digest, sig)
}
