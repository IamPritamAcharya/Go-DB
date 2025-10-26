package core

import (
	"encoding/binary"
	"errors"
)

const (
	BNODE_NODE = 1 // internal nodes (pointers in B+tree)
	BNODE_LEAf = 2 // Leaf node (stores values in B+tree)

	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

var (
	ErrKeyTooLarge = errors.New("key or value exceeds max size")
)

type BNode []byte

type BTree struct {
	root uint64
	get  func(uint64) []byte
	new  func([]byte) uint64
	del  func(uint64)
}

func NewBTree(get func(uint64) []byte, newPage func([]byte) uint64, del func(uint64)) *BTree {
	return &BTree{
		root: 0,
		get:  get,
		new:  newPage,
		del:  del,
	}
}

func checkLimit(key, val []byte) error {
	if len(key) > BTREE_MAX_KEY_SIZE || len(val) > BTREE_MAX_VAL_SIZE {
		return ErrKeyTooLarge
	}

	return nil
}

// Getter & setter header

func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

