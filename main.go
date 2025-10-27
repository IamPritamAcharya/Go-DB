package main

import (
	"acharyadb/core"
	"fmt"
)

func main() {

	mem := core.NewMemPages()

	db := core.NewKVWithMem(mem)

	_ = db.Set([]byte("a"), []byte("one"))
	_ = db.Set([]byte("b"), []byte("two"))
	_ = db.Set([]byte("c"), []byte("three"))

	for _, k := range []string{"a", "b", "c", "z"} {
		if v, ok := db.Get([]byte(k)); ok {
			fmt.Printf("Get %s -> %s\n", k, string(v))
		} else {
			fmt.Printf("Get %s -> <not found>\n", k)
		}
	}

	deleted, _ := db.Del([]byte("b"))
	fmt.Println("Deleted b?", deleted)

	if v, ok := db.Get([]byte("b")); ok {
		fmt.Println("b still exists:", string(v))
	} else {
		fmt.Println("b removed")
	}

	fmt.Printf("pages allocated: %d\n", mem.PagesAllocated())
}
