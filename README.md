# LibraDB

LibraDB is a simple, persistent key/value store written in pure Rust. The project aims to provide a working yet simple
example of a working database. If you're interested in databases, I encourage you to start here.

This is a re-implementation of the original version in Go: [Original Version in Pure GO](https://github.com/amit-davidson/LibraDB).

## Installing

To start using LibraDB, install Rust and get the repo:

```
git clone git@github.com:DavidDeCoding/LibraDB-rust.git
```

## Basic usage
```rust
use libradb_rust::{dal::{Options, DEFAULT_OPTIONS}, db::DB};

fn main() {
  let path = "libra.db";

    let options = Options {
        page_size: DEFAULT_OPTIONS.page_size,
        min_fill_percent: DEFAULT_OPTIONS.min_fill_percent,
        max_fill_percent: DEFAULT_OPTIONS.max_fill_percent,
        path
    };

    match DB::open(options) {
        Ok(db) => {
          let mut tx_writer = db.write_tx();

          match tx_writer.create_collection("test".to_string()) {
            Ok(collection) => {
              println!("Created Collection: {}", "test");
            }
            Err(error) => {
              panic!("Error: {:?}", error);
            }
          }
          match tx_writer.get_collection("test".to_string()) {
            Ok(Some(ref mut collection)) => {
              match collection.put("key1".to_string(), "value1".to_string(), &mut tx_writer) {
                  Ok(()) => {}
                  Err(error) => {
                      panic!("Error: {:?}", error);
                  }
              }
            }
            Ok(None) => {
              println!("Collection not found: {}", "test");
            }
            Err(error) => {
              panic!("Error: {:?}", error);
            }
          }
          
          match tx_writer.commit() {
              Ok(()) => {}
              Err(error) => {
                  panic!("Error: {:?}", error);
              }
          }
        }
        Err(error) => {
            panic!("Error: {:?}", error);
        }
    }
}
```
## Transactions
Read-only and read-write transactions are supported. LibraDB allows multiple read transactions or one read-write 
transaction at the same time. Transactions are goroutine-safe.

LibraDB has an isolation level: [Serializable](https://en.wikipedia.org/wiki/Isolation_(database_systems)#Serializable).
In simpler words, transactions are executed one after another and not at the same time.This is the highest isolation level.

### Read-write transactions

```rust
let mut tx_writer = db.write_tx();
...
match tx_writer.commit() {
    Ok(()) => {}
    Err(error) => {
        panic!("Error: {:?}", error);
    }
}
```
### Read-only transactions
```rust
let tx_reader = db.read_tx();
...
match tx_reader.commit() {
    Ok(_) => {}
    Err(error) => {
        panic!("Error: {:?}", error);
    }
}
```

## Collections
Collections are a grouping of key-value pairs. Collections are used to organize and quickly access data as each
collection is B-Tree by itself. All keys in a collection must be unique.
```rust
let mut tx_writer = db.write_tx();
match tx_writer.create_collection("test".to_string()) {
  Ok(collection) => {
    ...
  }
  Err(error) => {
    panic!("Error: {:?}", error);
  }
}
match tx_writer.commit() {
    Ok(()) => {}
    Err(error) => {
        panic!("Error: {:?}", error);
    }
}
```

### Auto generating ID
The `collection.id()` function returns an integer to be used as a unique identifier for key/value pairs.
```rust
let mut tx_writer = db.write_tx();

match tx_writer.get_collection("test".to_string()) {
  Ok(Some(ref mut collection)) => {
    println!("Collection ID: {}", collection.ID());
  }
  Ok(None) => {
    println!("Collection not found: {}", "test");
  }
  Err(error) => {
    panic!("Error: {:?}", error);
  }
}

match tx_writer.commit() {
    Ok(()) => {}
    Err(error) => {
        panic!("Error: {:?}", error);
    }
}
```
## Key-Value Pairs
Key/value pairs reside inside collections. CRUD operations are possible using the methods `collection.put` 
`collection.find` `collection.remove` as shown below.   
```rust
let mut tx_writer = db.write_tx();

match tx_writer.get_collection("test".to_string()) {
  Ok(Some(ref mut collection)) => {
    match collection.put("key1".to_string(), "value1".to_string(), &mut tx_writer) {
        Ok(()) => {}
        Err(error) => {
            panic!("Error: {:?}", error);
        }
    }
  }
  Ok(None) => {
    println!("Collection not found: {}", "test");
  }
  Err(error) => {
    panic!("Error: {:?}", error);
  }
}

match tx_writer.commit() {
    Ok(()) => {}
    Err(error) => {
        panic!("Error: {:?}", error);
    }
}
```
