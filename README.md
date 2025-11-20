# Go-DB

A lightweight educational database engine written in Go.
This project demonstrates how core database components work internally, including page management, B+ Tree indexing, and a basic key-value layer.

---

## Overview

Go-DB is a minimal database engine built from scratch using Go.
It is intended as a learning project to explore:

* B+ Tree index structures
* Memory-backed page storage
* Key-value operations
* Freelist management
* Node encoding and decoding
* A simple demonstration CLI

The codebase is small and approachable, making it suitable for anyone studying database internals.

---

## Features

### B+ Tree Index

* Supports insert, search, and split operations
* Fixed-size page layout (4 KB)
* Internal and leaf node types
* Structured key and pointer encoding
* Cascading splits up the tree

### In-Memory Page Store

* Simple page allocator using an incrementing page counter
* Thread-safe access through mutexes
* Separate storage for page bytes

### Key-Value Layer

* Provides `Set`, `Get`, and `Delete` operations
* Acts as a wrapper over the B+ Tree
* Useful as a higher-level storage interface

### Freelist Manager

* Tracks reusable pages
* Helps simulate real database page allocation

### Demonstration CLI

A small `main.go` file shows how the components interact and run together.

---

## Project Structure

```
Go-DB/
│
├── core/
│   ├── btree.go       # B+ Tree implementation
│   ├── kv.go          # Key-value store layer
│   ├── memstore.go    # In-memory page manager
│   └── freelist.go    # Freelist implementation
│
├── main.go            # Minimal CLI example
├── go.mod
└── README.md
```

---

## Getting Started

Clone the repository:

```bash
git clone https://github.com/your-username/Go-DB.git
cd Go-DB
```

Run the example:

```bash
go run main.go
```

---

## Example Usage

```go
store := core.NewMemStore()
tree := core.NewBTree(store)

_ = tree.Set([]byte("name"), []byte("Pritam"))
value, _ := tree.Get([]byte("name"))

fmt.Println(string(value)) // Output: Pritam
```

---

## Internal Architecture

### Page Layout

Every node is stored as a fixed-size byte slice.
The structure includes:

* Node type (leaf or internal)
* Key count
* Sorted keys
* Child pointers or value slots

### Node Splitting

If a node exceeds its capacity:

* A new sibling node is created
* The middle key is promoted
* Parent pointers are updated
* Splits propagate upward if needed

### Concurrency

`MemPages` and `KV` components use mutexes for safe concurrent access.
Lock scope is intentionally small to keep the design simple.

---

## Development Notes

Potential next steps and improvements:

* Replace `panic` calls with proper error handling
* Add unit tests for B+ Tree operations and KV behaviors
* Add benchmarks for insert/search performance
* Implement disk-backed page storage
* Add WAL or crash-recovery mechanisms
* Improve documentation for node layout and encoding

---

## Contributing

Contributions are welcome.
You may open issues, propose features, or submit pull requests.

---

## License

This project is licensed under the MIT License.


Just tell me what to add.

