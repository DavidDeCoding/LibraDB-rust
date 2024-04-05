use std::sync::RwLock;
use crate::{dal::{Options, DAL}, error::CustomError};


pub struct DB {
    rw_lock: RwLock<DAL>,
}

impl DB {

    pub fn open(options: Options) -> Result<DB, CustomError> {
        match DAL::new_dal(options) {
            Ok(dal) => Ok(DB {
                rw_lock: RwLock::new(dal)
            }),
            Err(error) => Err(error)
        }
    }

    pub fn read_tx(&mut self) -> Tx<'a> {
        let dal = self.rw_lock.read().unwrap();
        Tx {
            write: false,
            dal
        }
    }


}