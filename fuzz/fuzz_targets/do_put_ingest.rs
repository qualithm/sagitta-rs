//! Fuzz target: a raw gRPC `DoPut` request body through `SagittaService::do_put`.

#![no_main]

use std::sync::{Arc, OnceLock};

use arrow_flight::FlightData;
use arrow_flight::flight_service_server::FlightService;
use bytes::Bytes;
use http_body_util::Full;
use libfuzzer_sys::fuzz_target;
use sagitta::{AccessLevel, AuthToken, MemoryStore, SagittaService, User, UserStore};
use tonic::Request;
use tonic::codec::{Codec, Streaming};
use tonic::metadata::MetadataValue;
use tonic_prost::ProstCodec;

/// tonic's default limit on one decoded request message.
const MAX_MESSAGE_BYTES: usize = 4 * 1024 * 1024;

/// Grants write access to every token, so each input reaches the decoder.
struct Writer;

impl UserStore for Writer {
  fn authenticate(&self, username: &str, _password: &str) -> Option<User> {
    Some(User {
      username: username.to_string(),
      access: AccessLevel::FullAccess,
    })
  }

  fn user_for_token(&self, _token: &AuthToken) -> Option<User> {
    Some(User {
      username: "fuzz".to_string(),
      access: AccessLevel::FullAccess,
    })
  }

  fn create_token(&self, _user: &User) -> AuthToken {
    AuthToken::new("fuzz")
  }
}

/// One runtime for the whole run; each input gets a fresh store and service.
fn runtime() -> &'static tokio::runtime::Runtime {
  static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
  RUNTIME.get_or_init(|| {
    tokio::runtime::Builder::new_current_thread()
      .build()
      .expect("tokio runtime")
  })
}

fuzz_target!(|data: &[u8]| {
  runtime().block_on(async {
    let service =
      SagittaService::with_user_store(Arc::new(MemoryStore::new()), Arc::new(Writer)).await;

    // The body is gRPC-framed `FlightData` messages, as a client sends them.
    let mut codec = ProstCodec::<FlightData, FlightData>::default();
    let body = Full::new(Bytes::copy_from_slice(data));
    let stream = Streaming::new_request(codec.decoder(), body, None, Some(MAX_MESSAGE_BYTES));
    let mut request = Request::new(stream);
    request
      .metadata_mut()
      .insert("authorization", MetadataValue::from_static("Bearer fuzz"));

    let _ = service.do_put(request).await;
  });
});
