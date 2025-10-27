package core

import (
	"encoding/binary"
)

const (
	FREE_LIST_HEADER = 8
)

const FREE_LIST_CAP = (BTREE_PAGE_SIZE - FREE_LIST_HEADER) / 8

type FreeList struct {
	get func(uint64) []byte
	new func([]byte) uint64
	set func(uint64) []byte

	headPage uint64
	headSeq  uint64
	tailPage uint64
	tailSeq  uint64

	maxSeq uint64
}

func NewFreeList(get func(uint64) []byte, new func([]byte) uint64, set func(uint64) []byte) *FreeList {
	return &FreeList{get: get, new: new, set: set}
}

func seq2idx(seq uint64) int { return int(seq % FREE_LIST_CAP) }

func (fl *FreeList) HeadPage() uint64 { return fl.headPage }
func (fl *FreeList) HeadSeq() uint64  { return fl.headSeq }
func (fl *FreeList) TailPage() uint64 { return fl.tailPage }
func (fl *FreeList) TailSeq() uint64  { return fl.tailSeq }

func (fl *FreeList) SetHeadPage(v uint64) { fl.headPage = v }
func (fl *FreeList) SetHeadSeq(v uint64)  { fl.headSeq = v }
func (fl *FreeList) SetTailPage(v uint64) { fl.tailPage = v }
func (fl *FreeList) SetTailSeq(v uint64)  { fl.tailSeq = v }

func (fl *FreeList) SetMaxSeq() { fl.maxSeq = fl.tailSeq }

func (fl *FreeList) PopHead() uint64 {
	ptr, head := fl.flPop()
	if head != 0 {
		fl.PushTail(head)
	}
	return ptr
}

func (fl *FreeList) flPop() (ptr uint64, head uint64) {
	if fl.headSeq == fl.maxSeq {
		return 0, 0
	}
	node := LNode(fl.get(fl.headPage))
	ptr = node.getPtr(seq2idx(fl.headSeq))
	fl.headSeq++
	if seq2idx(fl.headSeq) == 0 {
		head = fl.headPage
		fl.headPage = node.getNext()
	}
	return
}

func (fl *FreeList) PushTail(ptr uint64) {

	LNode(fl.set(fl.tailPage)).setPtr(seq2idx(fl.tailSeq), ptr)
	fl.tailSeq++
	if seq2idx(fl.tailSeq) == 0 {

		next, head := fl.flPop()
		if next == 0 {
			next = fl.new(make([]byte, BTREE_PAGE_SIZE))
		}

		LNode(fl.set(fl.tailPage)).setNext(next)
		fl.tailPage = next
		if head != 0 {
			LNode(fl.set(fl.tailPage)).setPtr(0, head)
			fl.tailSeq++
		}
	}
}

type LNode []byte

func (n LNode) getNext() uint64  { return binary.LittleEndian.Uint64(n[0:8]) }
func (n LNode) setNext(x uint64) { binary.LittleEndian.PutUint64(n[0:8], x) }
func (n LNode) getPtr(i int) uint64 {
	pos := 8 + i*8
	return binary.LittleEndian.Uint64(n[pos : pos+8])
}
func (n LNode) setPtr(i int, p uint64) {
	pos := 8 + i*8
	binary.LittleEndian.PutUint64(n[pos:pos+8], p)
}
