package core

import (
	"sync"
)

type MemPages struct {
	mu    sync.Mutex
	pages map[uint64][]byte
	next  uint64
}

func NewMemPages() *MemPages {
	m := &MemPages{
		pages: make(map[uint64][]byte),
		next:  1,
	}

	return m
}

func (m *MemPages) Get(ptr uint64) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if node, ok := m.pages[ptr]; ok {
		return node
	}

	zero := make([]byte, BTREE_PAGE_SIZE)
	return zero
}

func (m *MemPages) New(node []byte) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	ptr := m.next
	cp := make([]byte, len(node))
	copy(cp, node)

	if len(cp) < BTREE_PAGE_SIZE {
		nn := make([]byte, BTREE_PAGE_SIZE)
		copy(nn, cp)
		cp = nn
	}
	m.pages[ptr] = cp
	m.next++
	return ptr
}

func (m *MemPages) Del(ptr uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pages, ptr)
}

func (m *MemPages) Write(ptr uint64) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	if node, ok := m.pages[ptr]; ok {
		return node
	}
	node := make([]byte, BTREE_PAGE_SIZE)
	m.pages[ptr] = node
	return node
}

func (m *MemPages) PagesAllocated() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.pages)
}
