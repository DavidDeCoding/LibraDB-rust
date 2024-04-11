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
            for i in 0..1000 {
                let mut tx_writer = db.write_tx();

                let collection_name = format!("collection_{}", i);
                let key = format!("key_{}", i);
                let value = format!("value_{}", i).as_bytes().to_owned();
                
                match tx_writer.create_collection(collection_name.clone()) {
                    Ok(ref mut collection) => {
                        match collection.put(key.clone(), value.clone(), &mut tx_writer) {
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

                match tx_writer.commit() {
                    Ok(()) => {}
                    Err(error) => {
                        panic!("Error: {:?}", error);
                    }
                }

                let tx_reader = db.read_tx();

                match tx_reader.get_collection(collection_name.to_string()) {
                    Ok(Some(ref mut collection)) => {
                        match collection.find(key.clone(), &tx_reader) {
                            Ok(Some(item)) => {
                                let actual_value = String::from_utf8(item.value);
                                let expected_value = String::from_utf8(value);
                                if actual_value != expected_value {
                                    panic!("Expected value: {:?}, Actual Value: {:?}", expected_value, actual_value);
                                }
                            }
                            Ok(None) => {
                                panic!("Item not found for key: {}", key.clone());
                            }
                            Err(error) => {
                                panic!("Error: {:?}", error);
                            }
                        }
                    }
                    Ok(None) => {
                        panic!("Failed to get collection: {}", collection_name);
                    }
                    Err(error) => {
                        panic!("Error: {:?}", error);
                    }
                }

                match tx_reader.commit() {
                    Ok(_) => {}
                    Err(error) => {
                        panic!("Error: {:?}", error);
                    }
                }
            }
        }
        Err(error) => {
            panic!("Error: {:?}", error);
        }
    }
}
