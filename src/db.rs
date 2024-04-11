use std::sync::RwLock;

use crate::{dal::{Options, DAL}, tx::{Tx, TxMut}, error::CustomError};


pub struct DB {
    pub dal: RwLock<DAL>,
}

impl DB {

    pub fn open(options: Options) -> Result<DB, CustomError> {
        match DAL::new_dal(options) {
            Ok(dal) => Ok(DB {
                dal: RwLock::new(dal),
            }),
            Err(error) => Err(error)
        }
    }

    pub fn read_tx(&self) -> Tx {
        Tx::new(self)
    }

    pub fn write_tx(&self) -> TxMut {
        TxMut::new(self)
    }
}


#[cfg(test)]
mod tests {
    use std::{fs, path::Path};

    use crate::dal::{Options, DEFAULT_OPTIONS};

    use super::DB;


    #[test]
    fn create_collection_put_item() {
        let options = Options {
            page_size: DEFAULT_OPTIONS.page_size,
            min_fill_percent: DEFAULT_OPTIONS.min_fill_percent,
            max_fill_percent: DEFAULT_OPTIONS.max_fill_percent,
            path: "./db_test_internal_1"
        };

        if Path::new(&options.path).exists() {
            match fs::remove_file(Path::new(&options.path)) {
                Ok(()) => {},
                Err(_) => {
                    assert!(false, "Failed to clean up db file");
                }
            }
        }

        let collection_name = "test_collection".to_string();
        match DB::open(options) {
            Ok(db) => {
                let mut tx = db.write_tx();

                match tx.create_collection(collection_name) {
                    Ok(ref mut collection) => {
                        match collection.put("0".to_string(), "1".as_bytes().to_owned(), &mut tx) {
                            Ok(()) => {}
                            Err(error) => {
                                assert!(false, "Put failed with error: {:?}", error);
                            }
                        }

                        match collection.find_mut("0".to_string(), &tx) {
                            Ok(Some(item)) => {
                                assert_eq!(item.key, "0".to_string());
                                assert_eq!(item.value, "1".as_bytes().to_owned());
                            }
                            Ok(None) => {
                                assert!(false, "No item found");
                            }
                            Err(error) => {
                                assert!(false, "Find failed with error: {:?}", error);
                            }
                        }

                        match tx.commit() {
                            Ok(()) => {}
                            Err(error) => {
                                assert!(false, "Transaction failed to commit with error: {:?}", error);
                            }
                        }
                    }
                    Err(error) => {
                        assert!(false, "Failed to create collection with error: {:?}", error);
                    }
                }
            }
            Err(error) => {
                assert!(false, "DB not opening with error: {:?}", error);
            }
        }
    }
}