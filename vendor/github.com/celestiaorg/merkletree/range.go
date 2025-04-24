package merkletree

import (
	"bytes"
	"hash"
	"io"
	"io/ioutil"
	"math"
	"math/bits"
)

// A LeafRange represents the contiguous set of leaves [Start,End).
type LeafRange struct {
	Start uint64
	End   uint64
}

// nextSubtreeSize returns the size of the subtree adjacent to start that does
// not overlap end.
func nextSubtreeSize(start, end uint64) int {
	ideal := bits.TrailingZeros64(start)
	max := bits.Len64(end-start) - 1
	if ideal > max {
		return 1 << uint(max)
	}
	return 1 << uint(ideal)
}

// validRangeSet checks whether a set of ranges is sorted and non-overlapping.
func validRangeSet(ranges []LeafRange) bool {
	for i, r := range ranges {
		if r.Start < 0 || r.Start >= r.End {
			return false
		}
		if i > 0 && ranges[i-1].End > r.Start {
			return false
		}
	}
	return true
}

// A SubtreeHasher calculates subtree roots in sequential order, for use with
// BuildRangeProof.
type SubtreeHasher interface {
	// NextSubtreeRoot returns the root of the next n leaves. If fewer than n
	// leaves are left in the tree, NextSubtreeRoot returns the root of those
	// leaves and nil. If no leaves are left, NextSubtreeRoot returns io.EOF.
	NextSubtreeRoot(n int) ([]byte, error)
	// Skip skips the next n leaves. If fewer than n leaves are left in the
	// tree, Skip returns io.ErrUnexpectedEOF. If exactly n leaves are left,
	// Skip returns nil (not io.EOF).
	Skip(n int) error
}

// ReaderSubtreeHasher implements SubtreeHasher by reading leaf data from an
// underlying stream.
type ReaderSubtreeHasher struct {
	r    io.Reader
	h    hash.Hash
	leaf []byte
}

// NextSubtreeRoot implements SubtreeHasher.
func (rsh *ReaderSubtreeHasher) NextSubtreeRoot(subtreeSize int) ([]byte, error) {
	tree := New(rsh.h)
	for i := 0; i < subtreeSize; i++ {
		n, err := io.ReadFull(rsh.r, rsh.leaf)
		if n > 0 {
			tree.Push(rsh.leaf[:n])
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break // reading a partial leaf is normal at the end of the stream
		} else if err != nil {
			return nil, err
		}
	}
	root := tree.Root()
	if root == nil {
		// we didn't read anything; return EOF to signal that there are no
		// more subtrees to hash.
		return nil, io.EOF
	}
	return root, nil
}

// Skip implements SubtreeHasher.
func (rsh *ReaderSubtreeHasher) Skip(n int) (err error) {
	skipSize := int64(len(rsh.leaf) * n)
	skipped, err := io.CopyN(ioutil.Discard, rsh.r, skipSize)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		if skipped == skipSize {
			return nil
		}
		return io.ErrUnexpectedEOF
	}
	return err
}

// NewReaderSubtreeHasher returns a new ReaderSubtreeHasher that reads leaf data from r.
func NewReaderSubtreeHasher(r io.Reader, leafSize int, h hash.Hash) *ReaderSubtreeHasher {
	return &ReaderSubtreeHasher{
		r:    r,
		h:    h,
		leaf: make([]byte, leafSize),
	}
}

// CachedSubtreeHasher implements SubtreeHasher using a set of precomputed
// leaf hashes.
type CachedSubtreeHasher struct {
	leafHashes [][]byte
	h          hash.Hash
}

// NextSubtreeRoot implements SubtreeHasher.
func (csh *CachedSubtreeHasher) NextSubtreeRoot(subtreeSize int) ([]byte, error) {
	if len(csh.leafHashes) == 0 {
		return nil, io.EOF
	}
	tree := New(csh.h)
	for i := 0; i < subtreeSize && len(csh.leafHashes) > 0; i++ {
		if err := tree.PushSubTree(0, csh.leafHashes[0]); err != nil {
			return nil, err
		}
		csh.leafHashes = csh.leafHashes[1:]
	}
	return tree.Root(), nil
}

// Skip implements SubtreeHasher.
func (csh *CachedSubtreeHasher) Skip(n int) error {
	if n > len(csh.leafHashes) {
		return io.ErrUnexpectedEOF
	}
	csh.leafHashes = csh.leafHashes[n:]
	return nil
}

// NewCachedSubtreeHasher creates a CachedSubtreeHasher using the specified
// leaf hashes and hash function.
func NewCachedSubtreeHasher(leafHashes [][]byte, h hash.Hash) *CachedSubtreeHasher {
	return &CachedSubtreeHasher{
		leafHashes: leafHashes,
		h:          h,
	}
}

// MixedSubtreeHasher implements SubtreeHasher by using cached subtree hashes
// when possible and otherwise reading leaf hashes from the underlying stream.
type MixedSubtreeHasher struct {
	csh           *CachedSubtreeHasher
	rsh           *ReaderSubtreeHasher
	leavesPerNode int
}

// NewMixedSubtreeHasher returns a new MixedSubtreeHasher that hashes nodeHashes
// which are already computed hashes of leavesPerNode leaves and also reads
// individual leaves from leafReader. The behavior of this implementation is
// greedy in regards to using the cached nodeHashes. A nodeHash will be consumed
// as soon as NextSubtreeRoot or Skip are called with a size greater than or
// equal to leavesPerNode.
func NewMixedSubtreeHasher(nodeHashes [][]byte, leafReader io.Reader, leavesPerNode int, leafSize int, h hash.Hash) *MixedSubtreeHasher {
	return &MixedSubtreeHasher{
		csh:           NewCachedSubtreeHasher(nodeHashes, h),
		rsh:           NewReaderSubtreeHasher(leafReader, leafSize, h),
		leavesPerNode: leavesPerNode,
	}
}

// Skip implements SubtreeHasher.
func (msh *MixedSubtreeHasher) Skip(n int) error {
	if n >= msh.leavesPerNode {
		return msh.csh.Skip(n / msh.leavesPerNode)
	}
	return msh.rsh.Skip(n)
}

// NextSubtreeRoot implements SubtreeHasher.
func (msh *MixedSubtreeHasher) NextSubtreeRoot(subtreeSize int) ([]byte, error) {
	// This will be hit if the current offset is aligned with the csh.
	if subtreeSize >= msh.leavesPerNode {
		return msh.csh.NextSubtreeRoot(subtreeSize / msh.leavesPerNode)
	}
	return msh.rsh.NextSubtreeRoot(subtreeSize)
}

// BuildMultiRangeProof constructs a proof for the specified leaf ranges, using
// the provided SubtreeHasher. The ranges must be sorted and non-overlapping.
func BuildMultiRangeProof(ranges []LeafRange, h SubtreeHasher) (proof [][]byte, err error) {
	if len(ranges) == 0 {
		return nil, nil
	}
	if !validRangeSet(ranges) {
		panic("BuildMultiRangeProof: illegal set of proof ranges")
	}

	// NOTE: this implementation is a bit magical. Essentially, the binary
	// property of Merkle trees allows us to determine which subtrees are
	// present in the proof just by looking at the binary representation of the
	// ranges.
	//
	// As an example, imagine we are constructing the following proof:
	//
	//               ┌────────┴────────┐
	//         ┌─────┴─────┐           │
	//      *──┴──┐     ┌──┴──*     ┌──┴──*
	//    ┌─┴─┐ *─┴─┐ ┌─┴─* ┌─┴─┐ *─┴─┐ ┌─┴─┐
	//    0   1 2   3 4   5 6   7 8   9 10  11
	//              ^^^               ^
	//
	// That is, a proof for ranges [3,5) and [9,10). Each * represents a hash
	// that should be included in the proof. But how do we find these *s?
	//
	// The high-level algorithm is as follows. We begin at leaf 0 and repeatedly
	// consume the largest possible subtree, stopping when we reach the
	// beginning of the first proof range. We then skip over the proof range,
	// and continue consuming until we reach the next range. Once all the ranges
	// have been processed, we finish by repeatedly consuming the largest
	// possible subtree until the end of the tree is reached.
	//
	// A "subtree" here means a set of leaves that comprise a single Merkle
	// root. In the diagram above, [0,1), [2,4), [0,8), and [11,12) are some of
	// the valid subtrees. To "consume" a subtree means to include its Merkle
	// root in the proof and advance past its leaves.
	//
	// Let's work through the algorithm for the proof above. We begin by
	// consuming the largest subtree that does not include leaf 3, which is
	// [0,2). We then consume the next largest subtree, [2,3). We have arrived
	// at the boundary of a proof range, so we skip over it, landing on leaf 5.
	// The largest subtree starting at leaf 5 is [5,6); after that, [6,8). Since
	// the next proof range begins at leaf 9, the next subtree is [8,9). We skip
	// over leaf 9 and consume the final subtree, [10,12), completing our proof.
	//
	// This appears to work, but one question remains: how do we determine what
	// the next largest subtree is?
	//
	// One thing we might notice is that when we start on an odd-indexed leaf,
	// e.g. 5, the subtree consists of just that leaf. This is because any other
	// subtree that includes leaf 5 must also include leaf 4. But since we can
	// only consume leaf 5 and beyond, we're stuck. Similarly, look at leaf 6.
	// We can consume leaf 7, forming the subtree [6,8), but any larger subtree
	// would have to include leaves 4 and 5. Again, we can't "move backwards,"
	// so the largest subtree has two leaves.
	//
	// It turns out that this property can be derived from the binary
	// representation of the leaf index: specifically, the least-significant 1
	// bit. Leaf 5, in binary, is 101; the 1 bit at 2^0 tells us that the
	// largest possible subtree has 2^0 leaves. Likewise, leaf 6 is 110; here,
	// the least-significant 1 bit is at 2^1, so the largest subtree has 2^1
	// leaves. Leaf 0, since it has no 1 bits, indicates a subtree of unbounded
	// size.
	//
	// But we have another limiting factor: the location of the next proof
	// range. So first we calculate the maximum possible subtree size, and then
	// divide it by 2 until it does not overlap the proof range. This completes
	// our nextSubtreeSize algorithm, and with it our full proof algorithm.

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

	// add proof hashes between proof ranges
	for _, r := range ranges {
		if err := consumeUntil(r.Start); err != nil {
			return nil, err
		}
		// skip leaves within proof range, one subtree at a time
		for leafIndex != r.End {
			subtreeSize := nextSubtreeSize(leafIndex, r.End)
			if err := h.Skip(subtreeSize); err != nil {
				return nil, err
			}
			leafIndex += uint64(subtreeSize)
		}
	}

	// keep adding proof hashes until we reach the end of the tree
	err = consumeUntil(math.MaxUint64)
	if err == io.EOF {
		err = nil // EOF is expected
	}
	return proof, err
}

// BuildRangeProof constructs a proof for the leaf range [proofStart,
// proofEnd) using the provided SubtreeHasher.
func BuildRangeProof(proofStart, proofEnd int, h SubtreeHasher) (proof [][]byte, err error) {
	if proofStart < 0 || proofStart > proofEnd || proofStart == proofEnd {
		panic("BuildRangeProof: illegal proof range")
	}
	return BuildMultiRangeProof([]LeafRange{{uint64(proofStart), uint64(proofEnd)}}, h)
}

// A LeafHasher returns the leaves of a Merkle tree in sequential order. When
// no more leaves are available, NextLeafHash must return io.EOF.
type LeafHasher interface {
	NextLeafHash() ([]byte, error)
}

// ReaderLeafHasher implements the LeafHasher interface by reading leaf data
// from the underlying stream.
type ReaderLeafHasher struct {
	r    io.Reader
	lh   LeafHasherz
	leaf []byte
}

// NextLeafHash implements LeafHasher.
func (rlh *ReaderLeafHasher) NextLeafHash() ([]byte, error) {
	n, err := io.ReadFull(rlh.r, rlh.leaf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return nil, err
	} else if n == 0 {
		return nil, io.EOF
	}
	return rlh.lh.HashLeaf(rlh.leaf[:n]), nil
}

// NewReaderLeafHasher creates a ReaderLeafHasher with the specified stream,
// hash, and leaf size.
func NewReaderLeafHasher(r io.Reader, h hash.Hash, leafSize int) *ReaderLeafHasher {
	return &ReaderLeafHasher{
		r:    r,
		lh:   NewDefaultHasher(h),
		leaf: make([]byte, leafSize),
	}
}

// CachedLeafHasher implements the LeafHasher interface by returning
// precomputed leaf hashes.
type CachedLeafHasher struct {
	leafHashes [][]byte
}

// NextLeafHash implements LeafHasher.
func (clh *CachedLeafHasher) NextLeafHash() ([]byte, error) {
	if len(clh.leafHashes) == 0 {
		return nil, io.EOF
	}
	h := clh.leafHashes[0]
	clh.leafHashes = clh.leafHashes[1:]
	return h, nil
}

// NewCachedLeafHasher creates a CachedLeafHasher from a set of precomputed
// leaf hashes.
func NewCachedLeafHasher(leafHashes [][]byte) *CachedLeafHasher {
	return &CachedLeafHasher{
		leafHashes: leafHashes,
	}
}

// VerifyMultiRangeProof verifies a proof produced by BuildMultiRangeProof
// using leaf hashes produced by lh, which must contain the concatenation of
// the leaf hashes within the proof ranges.
func VerifyMultiRangeProof(lh LeafHasher, h hash.Hash, ranges []LeafRange, proof [][]byte, root []byte) (bool, error) {
	if len(ranges) == 0 {
		return true, nil
	}
	if !validRangeSet(ranges) {
		panic("BuildMultiRangeProof: illegal set of proof ranges")
	}

	// manually build a tree using the proof hashes
	tree := New(h)
	var leafIndex uint64
	consumeUntil := func(end uint64) error {
		for leafIndex != end && len(proof) > 0 {
			subtreeSize := nextSubtreeSize(leafIndex, end)
			i := bits.TrailingZeros64(uint64(subtreeSize)) // log2
			if err := tree.PushSubTree(i, proof[0]); err != nil {
				// This *probably* should never happen, but just to guard
				// against adversarial inputs, return an error instead of
				// panicking.
				return err
			}
			proof = proof[1:]
			leafIndex += uint64(subtreeSize)
		}
		return nil
	}

	for _, r := range ranges {
		// add proof hashes from leaves [leafIndex, r.Start)
		if err := consumeUntil(r.Start); err != nil {
			return false, err
		}
		// add leaf hashes within the proof range
		for i := r.Start; i < r.End; i++ {
			leafHash, err := lh.NextLeafHash()
			if err != nil {
				return false, err
			}
			if err := tree.PushSubTree(0, leafHash); err != nil {
				panic(err)
			}
		}
		leafIndex += r.End - r.Start
	}

	// add remaining proof hashes after the last range ends
	if err := consumeUntil(math.MaxUint64); err != nil {
		return false, err
	}

	return bytes.Equal(tree.Root(), root), nil
}

// VerifyRangeProof verifies a proof produced by BuildRangeProof using leaf
// hashes produced by lh, which must contain only the leaf hashes within the
// proof range.
func VerifyRangeProof(lh LeafHasher, h hash.Hash, proofStart, proofEnd int, proof [][]byte, root []byte) (bool, error) {
	if proofStart < 0 || proofStart > proofEnd || proofStart == proofEnd {
		panic("VerifyRangeProof: illegal proof range")
	}
	return VerifyMultiRangeProof(lh, h, []LeafRange{{uint64(proofStart), uint64(proofEnd)}}, proof, root)
}

// proofMapping returns an index-to-index mapping that maps a hash's index in
// a "new" proof (produced by BuildRangeProof) to its index in an "old" proof
// (produced by (*Tree).Prove), i.e. new[i] = old[m[i]].
func proofMapping(proofSize, proofIndex int) (mapping []int) {
	// For context, the problem we're solving is that (*Tree).Prove constructs
	// proofs in a different way than the newer range proofs for a single
	// leaf. The proof hashes themselves are the same, of course, but the
	// *order* in which they appear in the proof is different. For example, in
	// the tree below, the two orderings of a proof for index 3 are:
	//
	//                       ┌─────────┴───────*
	//                 *─────┴─────┐           │
	//              ┌──┴──┐     *──┴──┐     ┌──┴──┐
	// Index:       0     1     2     3     4     5
	// Old Proof:      1        0              2
	// New Proof:      0        1              2
	//
	// In other words, the old proofs proceed "bottom-up", tracing the path
	// from the proofIndex to the root of the tree, whereas the new proofs
	// proceed "left-to-right."
	//
	// There is a simple algorithm for converting old proofs to new proofs.
	// First, we iterate through the bits of the proofIndex; if the i'th bit
	// is a 0, we add to the "right-side" hashes; if it's a 1, we add it to
	// the "left-side". Then we just need to reverse the order of the left-
	// side hashes (see the comment in BuildRangeProof) and concatenate the
	// left side with the right side.
	//
	// Unfortunately, this algorithm only works for balanced trees (trees with
	// 2^n leaves). Consider a proof for index 4 in the above tree. The actual
	// proof should contain only two hashes, but the naive algorithm would
	// generate three -- one for each level. More specifically: the bits of 4
	// are 001, so the algorithm would see "right-side, right-side, left-
	// side." But after the first "right-side", there are no more leaves left
	// on the right side!
	//
	// So we have to augment the algorithm to be aware of these "missing
	// levels." Fortunately, we can exploit a property of unbalanced trees to
	// accomplish this without too much trouble. The property is: if a proof
	// is missing n hashes, they are always the hashes of the n largest right-
	// side subtrees. Or, stated another way: the proof will only include the
	// m *smallest* right-side subtrees. For example, we know that the proof
	// for index 4 contains only one right-side subtree hash; using the
	// property, we can be confident that the hash is of a single leaf.
	//
	// This lends itself to an easy change to the algorithm: simply stop
	// adding right-side hashes after we've hit the known limit. But how do we
	// know what the limit is? Easy: we know that there's a 1 bit in the
	// proofIndex for each left-side hash, so we just subtract the number of 1
	// bits from the total number of proof hashes.
	numRights := proofSize - bits.OnesCount(uint(proofIndex))
	var left, right []int
	for i := 0; len(left)+len(right) < proofSize; i++ {
		subtreeSize := 1 << uint64(i)
		if proofIndex&subtreeSize != 0 {
			// appending len(left)+len(right) is a little trick to ensure
			// that, whether we append to left or right, the combined sequence
			// is 0,1,2,3...
			left = append(left, len(left)+len(right))
		} else if len(right) < numRights {
			right = append(right, len(left)+len(right))
		}
	}
	// left-side needs to be reversed
	for i := range left {
		mapping = append(mapping, left[len(left)-i-1])
	}
	return append(mapping, right...)
}

// ConvertSingleProofToRangeProof converts a proof produced by (*Tree).Prove
// to a single-leaf range proof. proofIndex must be >= 0.
func ConvertSingleProofToRangeProof(proof [][]byte, proofIndex int) [][]byte {
	newproof := make([][]byte, len(proof))
	mapping := proofMapping(len(proof), proofIndex)
	for i, j := range mapping {
		newproof[i] = proof[j]
	}
	return newproof
}

// ConvertRangeProofToSingleProof converts a single-leaf range proof to the
// equivalent proof produced by (*Tree).Prove. proofIndex must be >= 0.
func ConvertRangeProofToSingleProof(proof [][]byte, proofIndex int) [][]byte {
	oldproof := make([][]byte, len(proof))
	mapping := proofMapping(len(proof), proofIndex)
	for i, j := range mapping {
		oldproof[j] = proof[i]
	}
	return oldproof
}
