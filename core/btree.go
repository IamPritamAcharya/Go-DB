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

// Getter & setter header for decoding on the fly

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

func (node BNode) getPtr(idx uint16) uint64 {
	if idx >= node.nkeys() {
		panic("getPtr idx out of range")
	}

	pos := 4 + int(idx)*8 // first 4 bytes are the headers (0-1 are btype and 2-3 are nkeys) and idx*8 each pointer is 8 byptes long i.e. uint64

	return binary.LittleEndian.Uint64(node[pos : pos+8])
}

func (node BNode) setPtr(idx uint16, val uint64) {
	if idx >= node.nkeys() {
		panic("setPtr idx out of range")
	}

	pos := 4 + int(idx)*8
	binary.LittleEndian.PutUint64(node[pos:pos+8], val)
}

func (node BNode) getOffset(idx uint16) uint16 {
	if idx == 0 {
		return 0
	}

	n := node.nkeys()
	if idx > n {
		panic("offset idx > nkeys")
	}

	pos := 4 + int(n)*8 + int(idx-1)*2
	return binary.LittleEndian.Uint16(node[pos : pos+2])
}

func (node BNode) setOffset(idx uint16, v uint16) {
	n := node.nkeys()
	pos := 4 + int(n)*8 + int(idx-1)*2
	binary.LittleEndian.PutUint16(node[pos:pos+2], v)
}

func (node BNode) kvPos(idx uint16) uint16 {
	n := node.nkeys()
	return uint16(4+int(n)*8+int(n)*2) + node.getOffset(idx)
}

func (node BNode) getKey(idx uint16) []byte {
	if idx >= node.nkeys() {
		return nil
	}

	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos : pos+2])
	return node[pos+4 : pos+4+uint16(klen)]
}

func (node BNode) getVal(idx uint16) []byte {
	if idx >= node.nkeys() {
		return nil
	}

	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos : pos+2])
	vlen := binary.LittleEndian.Uint16(node[pos+2 : pos+4])

	return node[pos+4+uint16(klen) : pos+4+uint16(klen)+uint16(vlen)]
}

func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	new.setPtr(idx, ptr)
	pos := new.kvPos(idx)
	binary.LittleEndian.PutUint16(new[pos+0:pos+2], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:pos+4], uint16(len(val)))
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)
	new.setOffset(idx+1, new.getOffset(idx)+uint16(4+len(key)+len(val)))
}


func nodeAppendRange(new BNode, old BNode, dstNew uint16, scrOld uint16, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst := dstNew + i
		src := scrOld + i
		nodeAppendKV(new, dst, old.getPtr(src), old.getKey(src), old.getVal(src))
	}
}