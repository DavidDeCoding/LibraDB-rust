use crate::dal::DAL;

pub struct Tx<'a> {

    write: bool,
    dal: &'a DAL,
}