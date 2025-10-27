package core

import (
	"bytes"
	"encoding/binary"
	"errors"
)

const (
	BNODE_NODE = 1
	BNODE_LEAF = 2

	BTREE_PAGE_SIZE    = 4096
	BTREE_MAX_KEY_SIZE = 1000
	BTREE_MAX_VAL_SIZE = 3000
)

var ErrKeyTooLarge = errors.New("key or value exceeds max size")

type BNode []byte

type BTree struct {
	root uint64
	get  func(uint64) []byte
	new  func([]byte) uint64
	del  func(uint64)
}

func NewBTree(get func(uint64) []byte, newPage func([]byte) uint64, del func(uint64)) *BTree {
	return &BTree{root: 0, get: get, new: newPage, del: del}
}

func checkLimit(key, val []byte) error {
	if len(key) > BTREE_MAX_KEY_SIZE || len(val) > BTREE_MAX_VAL_SIZE {
		return ErrKeyTooLarge
	}
	return nil
}

func (node BNode) btype() uint16 { return binary.LittleEndian.Uint16(node[0:2]) }
func (node BNode) nkeys() uint16 { return binary.LittleEndian.Uint16(node[2:4]) }
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

func (node BNode) getPtr(idx uint16) uint64 {
	if idx >= node.nkeys() {
		panic("getPtr idx out of range")
	}
	pos := 4 + int(idx)*8
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
		panic("getOffset idx > nkeys")
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

func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst := dstNew + i
		src := srcOld + i
		nodeAppendKV(new, dst, old.getPtr(src), old.getKey(src), old.getVal(src))
	}
}

func nodeLookupLE(node BNode, key []byte) uint16 {
	n := node.nkeys()
	if n == 0 {
		return 0
	}
	var i uint16
	for i = 0; i < n; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp == 0 {
			return i
		}
		if cmp > 0 {
			if i == 0 {
				return 0
			}
			return i - 1
		}
	}
	return n - 1
}

func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx)
}

func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

func nodeSplit2(left BNode, right BNode, old BNode) {
	if old.nkeys() < 2 {
		panic("nodeSplit2 requires >=2 keys")
	}
	nleft := old.nkeys() / 2
	leftBytes := func() uint16 {
		return 4 + 8*nleft + 2*nleft + old.getOffset(nleft)
	}
	for leftBytes() > BTREE_PAGE_SIZE {
		nleft--
	}
	if nleft == 0 {
		panic("cannot fit any key on left")
	}
	rightBytes := func() uint16 {
		return old.nbytes() - leftBytes() + 4
	}
	for rightBytes() > BTREE_PAGE_SIZE {
		nleft++
	}
	if !(nleft < old.nkeys()) {
		panic("split invariant")
	}
	nright := old.nkeys() - nleft
	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)
}

func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old, nil, nil}
	}
	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)
	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right, nil}
	}
	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)
	return 3, [3]BNode{leftleft, middle, right}
}

func (t *BTree) Insert(key, val []byte) error {
	if err := checkLimit(key, val); err != nil {
		return err
	}
	if t.root == 0 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)
		nodeAppendKV(root, 0, 0, nil, nil)
		nodeAppendKV(root, 1, 0, key, val)
		t.root = t.new(root)
		return nil
	}
	node := BNode(t.get(t.root))
	newNode := treeInsert(t, node, key, val)
	nsplit, split := nodeSplit3(newNode)
	t.del(t.root)
	if nsplit > 1 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)
		for i := 0; i < int(nsplit); i++ {
			ptr := t.new(split[i])
			nodeAppendKV(root, uint16(i), ptr, split[i].getKey(0), nil)
		}
		t.root = t.new(root)
	} else {
		t.root = t.new(split[0])
	}
	return nil
}

func treeInsert(t *BTree, node BNode, key []byte, val []byte) BNode {
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))
	idx := nodeLookupLE(node, key)
	switch node.btype() {
	case BNODE_LEAF:
		if idx < node.nkeys() && bytes.Equal(node.getKey(idx), key) {
			leafUpdate(new, node, idx, key, val)
		} else {
			leafInsert(new, node, idx+1, key, val)
		}
	case BNODE_NODE:
		ptr := node.getPtr(idx)
		knode := BNode(t.get(ptr))
		kn := treeInsert(t, knode, key, val)
		nsplit, split := nodeSplit3(kn)
		t.del(ptr)
		nodeReplaceKidN(t, new, node, idx, split[:nsplit]...)
	default:
		panic("unknown node type")
	}
	return new
}

func nodeReplaceKidN(t *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))
	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)
	for i := range kids {
		ptr := t.new(kids[i])
		nodeAppendKV(new, idx+uint16(i), ptr, kids[i].getKey(0), nil)
	}
	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

func (t *BTree) Get(key []byte) ([]byte, bool) {
	if t.root == 0 {
		return nil, false
	}
	node := BNode(t.get(t.root))
	for {
		idx := nodeLookupLE(node, key)
		if node.btype() == BNODE_LEAF {
			if idx < node.nkeys() && bytes.Equal(node.getKey(idx), key) {
				return node.getVal(idx), true
			}
			return nil, false
		}
		ptr := node.getPtr(idx)
		node = BNode(t.get(ptr))
	}
}

func leafDelete(new BNode, old BNode, idx uint16) {
	new.setHeader(BNODE_LEAF, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendRange(new, old, idx, idx+1, old.nkeys()-(idx+1))
}

func nodeMerge(new BNode, left BNode, right BNode) {
	new.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(new, left, 0, 0, left.nkeys())
	nodeAppendRange(new, right, left.nkeys(), 0, right.nkeys())
}

func nodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, nil)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+2))
}

func shouldMerge(t *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, nil
	}

	if idx > 0 {
		sib := BNode(t.get(node.getPtr(idx - 1)))
		merged := sib.nbytes() + updated.nbytes() - 4
		if merged <= BTREE_PAGE_SIZE {
			return -1, sib
		}
	}

	if idx+1 < node.nkeys() {
		sib := BNode(t.get(node.getPtr(idx + 1)))
		merged := sib.nbytes() + updated.nbytes() - 4
		if merged <= BTREE_PAGE_SIZE {
			return +1, sib
		}
	}
	return 0, nil
}

func treeDelete(t *BTree, node BNode, key []byte) BNode {

	if node.btype() == BNODE_LEAF {
		idx := nodeLookupLE(node, key)
		if idx >= node.nkeys() || !bytes.Equal(node.getKey(idx), key) {
			return nil
		}
		new := BNode(make([]byte, BTREE_PAGE_SIZE))
		leafDelete(new, node, idx)
		return new
	}

	idx := nodeLookupLE(node, key)
	kptr := node.getPtr(idx)
	updated := treeDelete(t, BNode(t.get(kptr)), key)
	if len(updated) == 0 {
		return nil
	}
	t.del(kptr)
	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	mergeDir, sibling := shouldMerge(t, node, idx, updated)
	switch {
	case mergeDir < 0:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		t.del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, t.new(merged), merged.getKey(0))
	case mergeDir > 0:
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		t.del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, t.new(merged), merged.getKey(0))
	case mergeDir == 0 && updated.nkeys() == 0:

		new.setHeader(BNODE_NODE, 0)
	case mergeDir == 0 && updated.nkeys() > 0:
		nodeReplaceKidN(t, new, node, idx, updated)
	}
	return new
}

func (t *BTree) Delete(key []byte) (bool, error) {
	if t.root == 0 {
		return false, nil
	}
	node := BNode(t.get(t.root))
	updated := treeDelete(t, node, key)
	if len(updated) == 0 {
		return false, nil
	}
	nsplit, split := nodeSplit3(updated)
	t.del(t.root)
	if nsplit > 1 {
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)
		for i := 0; i < int(nsplit); i++ {
			ptr := t.new(split[i])
			nodeAppendKV(root, uint16(i), ptr, split[i].getKey(0), nil)
		}
		t.root = t.new(root)
	} else {
		t.root = t.new(split[0])
	}
	return true, nil
}
