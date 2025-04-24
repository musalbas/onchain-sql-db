package merkletree

import "hash"

type LeafHasherz interface {
	HashLeaf(leaf []byte) []byte
}
type NodeHasher interface {
	HashNode(l, r []byte) []byte
}
type TreeHasher interface {
	LeafHasherz
	NodeHasher
}

var _ TreeHasher = &DefaultTreeHasher{}

type DefaultTreeHasher struct {
	h hash.Hash
}

func NewDefaultHasher(h hash.Hash) *DefaultTreeHasher {
	return &DefaultTreeHasher{h}
}

func (d *DefaultTreeHasher) HashLeaf(leaf []byte) []byte {
	return sum(d.h, leafHashPrefix, leaf)
}

func (d *DefaultTreeHasher) HashNode(l, r []byte) []byte {
	return sum(d.h, nodeHashPrefix, l, r)
}
