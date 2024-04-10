use crate::{dal::{Options, DAL}, tx::Tx, error::CustomError};


pub struct DB {
    pub dal: DAL,
}

impl DB {

    pub fn open(options: Options) -> Result<DB, CustomError> {
        match DAL::new_dal(options) {
            Ok(dal) => Ok(DB {
                dal,
            }),
            Err(error) => Err(error)
        }
    }

    pub fn read_tx(&mut self) -> Tx {
        Tx::new(self, false)
    }

    pub fn write_tx(&mut self) -> Tx {
        Tx::new(self, true)
    }
}