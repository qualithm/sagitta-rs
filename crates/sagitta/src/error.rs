//! Error types for Sagitta.

use arrow_flight::error::FlightError;
use tonic::Status;

/// Result type alias for Sagitta operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Sagitta error types.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Dataset not found.
    #[error("dataset not found: {0}")]
    NotFound(String),

    /// Invalid request from client.
    #[error("invalid request: {0}")]
    InvalidRequest(String),

    /// Authentication failed.
    #[error("authentication failed: {0}")]
    Unauthenticated(String),

    /// Permission denied.
    #[error("permission denied: {0}")]
    PermissionDenied(String),

    /// Feature not implemented.
    #[error("not implemented: {0}")]
    Unimplemented(String),

    /// Transaction conflict or aborted operation.
    #[error("aborted: {0}")]
    Aborted(String),

    /// Internal server error.
    #[error("internal error: {0}")]
    Internal(String),

    /// Transient failure, client should retry.
    #[error("unavailable: {0}")]
    Unavailable(String),

    /// Operation timed out.
    #[error("deadline exceeded: {0}")]
    DeadlineExceeded(String),

    /// Client cancelled the operation.
    #[error("cancelled: {0}")]
    Cancelled(String),

    /// Rate limited or resource exhausted.
    #[error("resource exhausted: {0}")]
    ResourceExhausted(String),

    /// Arrow error.
    #[error("arrow error: {0}")]
    Arrow(#[from] Box<arrow_schema::ArrowError>),

    /// Arrow Flight error.
    #[error("flight error: {0}")]
    Flight(#[from] Box<FlightError>),
}

impl From<arrow_schema::ArrowError> for Error {
    fn from(err: arrow_schema::ArrowError) -> Self {
        Error::Arrow(Box::new(err))
    }
}

impl From<FlightError> for Error {
    fn from(err: FlightError) -> Self {
        Error::Flight(Box::new(err))
    }
}

impl From<Error> for Status {
    fn from(err: Error) -> Self {
        match err {
            Error::NotFound(msg) => Status::not_found(msg),
            Error::InvalidRequest(msg) => Status::invalid_argument(msg),
            Error::Unauthenticated(msg) => Status::unauthenticated(msg),
            Error::PermissionDenied(msg) => Status::permission_denied(msg),
            Error::Unimplemented(msg) => Status::unimplemented(msg),
            Error::Aborted(msg) => Status::aborted(msg),
            Error::Internal(msg) => Status::internal(msg),
            Error::Unavailable(msg) => Status::unavailable(msg),
            Error::DeadlineExceeded(msg) => Status::deadline_exceeded(msg),
            Error::Cancelled(msg) => Status::cancelled(msg),
            Error::ResourceExhausted(msg) => Status::resource_exhausted(msg),
            Error::Arrow(e) => Status::internal(e.to_string()),
            Error::Flight(e) => Status::internal(e.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::Code;

    #[test]
    fn test_not_found_maps_to_grpc_not_found() {
        let err = Error::NotFound("table".to_string());
        let status: Status = err.into();
        assert_eq!(status.code(), Code::NotFound);
    }

    #[test]
    fn test_invalid_request_maps_to_grpc_invalid_argument() {
        let err = Error::InvalidRequest("bad SQL".to_string());
        let status: Status = err.into();
        assert_eq!(status.code(), Code::InvalidArgument);
    }

    #[test]
    fn test_unauthenticated_maps_to_grpc_unauthenticated() {
        let err = Error::Unauthenticated("invalid credentials".to_string());
        let status: Status = err.into();
        assert_eq!(status.code(), Code::Unauthenticated);
    }

    #[test]
    fn test_permission_denied_maps_to_grpc_permission_denied() {
        let err = Error::PermissionDenied("read-only user".to_string());
        let status: Status = err.into();
        assert_eq!(status.code(), Code::PermissionDenied);
    }

    #[test]
    fn test_unimplemented_maps_to_grpc_unimplemented() {
        let err = Error::Unimplemented("feature X".to_string());
        let status: Status = err.into();
        assert_eq!(status.code(), Code::Unimplemented);
    }

    #[test]
    fn test_aborted_maps_to_grpc_aborted() {
        let err = Error::Aborted("transaction conflict".to_string());
        let status: Status = err.into();
        assert_eq!(status.code(), Code::Aborted);
    }

    #[test]
    fn test_internal_maps_to_grpc_internal() {
        let err = Error::Internal("server error".to_string());
        let status: Status = err.into();
        assert_eq!(status.code(), Code::Internal);
    }

    #[test]
    fn test_unavailable_maps_to_grpc_unavailable() {
        let err = Error::Unavailable("service restarting".to_string());
        let status: Status = err.into();
        assert_eq!(status.code(), Code::Unavailable);
    }

    #[test]
    fn test_deadline_exceeded_maps_to_grpc_deadline_exceeded() {
        let err = Error::DeadlineExceeded("query timeout".to_string());
        let status: Status = err.into();
        assert_eq!(status.code(), Code::DeadlineExceeded);
    }

    #[test]
    fn test_cancelled_maps_to_grpc_cancelled() {
        let err = Error::Cancelled("client cancelled".to_string());
        let status: Status = err.into();
        assert_eq!(status.code(), Code::Cancelled);
    }

    #[test]
    fn test_resource_exhausted_maps_to_grpc_resource_exhausted() {
        let err = Error::ResourceExhausted("rate limited".to_string());
        let status: Status = err.into();
        assert_eq!(status.code(), Code::ResourceExhausted);
    }
}
