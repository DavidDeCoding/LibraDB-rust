#[derive(Debug, Clone)]
pub struct CustomError {
    pub message: String
}

impl CustomError {

    pub fn new(message: String) -> CustomError {
        CustomError {
            message
        }
    }
}