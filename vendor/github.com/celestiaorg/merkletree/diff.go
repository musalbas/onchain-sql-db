package merkletree

import (
	"bytes"
	"hash"
	"io"
	"math/bits"
)

// BuildDiffProof constructs a Merkle diff for the specified leaf ranges, using
// the provided SubtreeHasher. The ranges must be sorted and non-overlapping.
func BuildDiffProof(ranges []LeafRange, h SubtreeHasher, numLeaves uint64) (proof [][]byte, err error) {
	// This code is a direct copy of the BuildMultiRangeProof code, except that
	// it ends by consuming until numLeaves instead of math.MaxUint64. This can
	// result in a larger proof, but the extra proof hashes are required for
	// certain diffs.
	if !validRangeSet(ranges) {
		panic("BuildDiffProof: illegal set of proof ranges")
	}
	var leafIndex uint64
	consumeUntil := func(end uint64) error {
		for leafIndex != end {
			subtreeSize := nextSubtreeSize(leafIndex, end)
			root, err := h.NextSubtreeRoot(subtreeSize)
			if err != nil {
				return err
			}
			proof = append(proof, root)
			leafIndex += uint64(subtreeSize)
		}
		return nil
	}
	for _, r := range ranges {
		if err := consumeUntil(r.Start); err != nil {
			return nil, err
		}
		for leafIndex != r.End {
			subtreeSize := nextSubtreeSize(leafIndex, r.End)
			if err := h.Skip(subtreeSize); err != nil {
				return nil, err
			}
			leafIndex += uint64(subtreeSize)
		}
	}
	err = consumeUntil(numLeaves)
	if err == io.EOF {
		err = nil
	}
	return proof, err
}

// CompressLeafHashes takes the ranges of modified leaves as an input together
// with a SubtreeHasher which can produce all modified leaf hashes to compress
// the leaf hashes into subtrees where possible. These compressed leaf hashes
// can be used as the 'rangeHashes' input to VerifyDiffProof.
func CompressLeafHashes(ranges []LeafRange, h SubtreeHasher) (compressed [][]byte, err error) {
	if !validRangeSet(ranges) {
		panic("BuildDiffProof: illegal set of proof ranges")
	}
	for _, r := range ranges {
		for leafIndex := r.Start; leafIndex != r.End; {
			subtreeSize := nextSubtreeSize(leafIndex, r.End)
			root, err := h.NextSubtreeRoot(subtreeSize)
			if err != nil {
				return nil, err
			}
			compressed = append(compressed, root)
			leafIndex += uint64(subtreeSize)
		}
	}
	return
}

// VerifyDiffProof verifies a proof produced by BuildDiffProof using subtree
// hashes produced by sh, which must contain the concatenation of the subtree
// hashes within the proof ranges.
func VerifyDiffProof(rangeHashes [][]byte, numLeaves uint64, h hash.Hash, ranges []LeafRange, proof [][]byte, root []byte) (bool, error) {
	if !validRangeSet(ranges) {
		panic("VerifyDiffProof: illegal set of proof ranges")
	}
	tree := New(h)
	var leafIndex uint64
	consumeUntil := func(end uint64, hashes *[][]byte) error {
		for leafIndex != end && len(*hashes) > 0 {
			subtreeSize := nextSubtreeSize(leafIndex, end)
			i := bits.TrailingZeros64(uint64(subtreeSize))
			if err := tree.PushSubTree(i, (*hashes)[0]); err != nil {
				return err
			}
			*hashes = (*hashes)[1:]
			leafIndex += uint64(subtreeSize)
		}
		return nil
	}
	for _, r := range ranges {
		if err := consumeUntil(r.Start, &proof); err != nil {
			return false, err
		}
		if err := consumeUntil(r.End, &rangeHashes); err != nil {
			return false, err
		}
	}
	err := consumeUntil(numLeaves, &proof)
	return bytes.Equal(tree.Root(), root), err
}
