package core

import (
	"encoding/binary"
	"errors"
	"sync"
)

const DB_SIG = "BuildYourOwnDB06"

type KV struct {
	mem *MemPages

	tree *BTree
	free *FreeList

	page struct {
		mu      sync.Mutex
		flushed uint64
		temp    [][]byte
		updates map[uint64][]byte
	}

	failed bool
}

func NewKVWithMem(mem *MemPages) *KV {
	db := &KV{mem: mem}

	db.page.flushed = 1
	db.page.updates = make(map[uint64][]byte)

	db.tree = NewBTree(mem.Get, mem.New, mem.Del)

	db.free = NewFreeList(mem.Get, mem.New, mem.Write)
	return db
}

func (db *KV) Get(key []byte) ([]byte, bool) {
	return db.tree.Get(key)
}

func (db *KV) Set(key, val []byte) error {
	if err := checkLimit(key, val); err != nil {
		return err
	}

	if err := db.tree.Insert(key, val); err != nil {
		return err
	}

	return nil
}

func (db *KV) Del(key []byte) (bool, error) {
	deleted, err := db.tree.Delete(key)
	if err != nil {
		return false, err
	}
	return deleted, nil
}

func saveMetaInBytes(db *KV) []byte {
	var buf [BTREE_PAGE_SIZE]byte
	copy(buf[0:16], []byte(DB_SIG))
	binary.LittleEndian.PutUint64(buf[16:], db.tree.root)
	binary.LittleEndian.PutUint64(buf[24:], db.page.flushed)

	return buf[:]
}

func loadMetaFromBytes(db *KV, data []byte) error {
	if len(data) < 32 {
		return errors.New("meta too small")
	}
	root := binary.LittleEndian.Uint64(data[16:])
	db.tree.root = root
	db.page.flushed = binary.LittleEndian.Uint64(data[24:])
	return nil
}
