//! Pre-decode checks on incoming Arrow IPC messages.

use arrow_flight::FlightData;
use arrow_flight::error::FlightError;
use arrow_ipc::{MessageHeader, root_as_message};
use futures::{Stream, StreamExt};

/// tonic's default limit on one decoded request message.
const MAX_MESSAGE_BYTES: u64 = 4 * 1024 * 1024;

/// The most a single compressed IPC message may declare it decompresses to, in bytes.
pub(crate) const MAX_DECOMPRESSED_MESSAGE_BYTES: u64 = 64 * MAX_MESSAGE_BYTES;

/// Length of the uncompressed-size prefix on each compressed body buffer.
const PREFIX_BYTES: usize = 8;

/// Refuses a compressed record or dictionary batch whose buffers declare more than `cap`
/// decompressed bytes in all.
///
/// The decoder allocates each buffer's declared length before decompressing it, so a crafted
/// prefix would otherwise abort the process. A compressed buffer outside the body is refused as
/// well; messages this can't parse are left to the decoder.
///
/// # Errors
///
/// Returns a protocol error naming the declared and allowed sizes.
pub(crate) fn check_decompressed_size(data: &FlightData, cap: u64) -> Result<(), FlightError> {
  let Ok(message) = root_as_message(&data.data_header) else {
    return Ok(());
  };
  let batch = match message.header_type() {
    MessageHeader::RecordBatch => message.header_as_record_batch(),
    MessageHeader::DictionaryBatch => message
      .header_as_dictionary_batch()
      .and_then(|dictionary| dictionary.data()),
    _ => None,
  };
  let Some(batch) = batch else {
    return Ok(());
  };
  if batch.compression().is_none() {
    return Ok(());
  }
  let Some(buffers) = batch.buffers() else {
    return Ok(());
  };

  let body = &data.data_body;
  let mut declared: u64 = 0;
  for buffer in buffers.iter() {
    if buffer.length() == 0 {
      continue;
    }
    // The decoder slices each buffer from the body unchecked, so one outside it is refused too.
    let buffer_bytes = usize::try_from(buffer.offset())
      .ok()
      .zip(usize::try_from(buffer.length()).ok())
      .filter(|&(_, length)| length >= PREFIX_BYTES)
      .and_then(|(start, length)| body.get(start..start.checked_add(length)?));
    let Some(buffer_bytes) = buffer_bytes else {
      return Err(FlightError::ProtocolError(
        "compressed ipc buffer lies outside the message body".to_string(),
      ));
    };
    let prefix = &buffer_bytes[..PREFIX_BYTES];
    let length = i64::from_le_bytes(prefix.try_into().expect("prefix is eight bytes"));
    // -1 marks a buffer stored uncompressed; any other negative length is malformed.
    if length == -1 {
      continue;
    }
    let length = u64::try_from(length).map_err(|_| {
      FlightError::ProtocolError("compressed ipc buffer declares a negative length".to_string())
    })?;
    declared = declared.saturating_add(length);
    if declared > cap {
      return Err(FlightError::ProtocolError(format!(
        "compressed ipc message declares over {cap} decompressed bytes"
      )));
    }
  }
  Ok(())
}

/// Applies [`check_decompressed_size`] to every message of a Flight stream before it is decoded.
pub(crate) fn guard_decompressed_size<S>(
  stream: S,
) -> impl Stream<Item = Result<FlightData, FlightError>> + Send
where
  S: Stream<Item = Result<FlightData, FlightError>> + Send,
{
  stream.map(|item| {
    let data = item?;
    check_decompressed_size(&data, MAX_DECOMPRESSED_MESSAGE_BYTES)?;
    Ok(data)
  })
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use arrow_array::{Int64Array, RecordBatch};
  use arrow_flight::encode::FlightDataEncoderBuilder;
  use arrow_ipc::CompressionType;
  use arrow_ipc::writer::IpcWriteOptions;
  use arrow_schema::{DataType, Field, Schema};
  use futures::TryStreamExt;

  use super::*;

  async fn encode(compression: Option<CompressionType>) -> Vec<FlightData> {
    let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
      schema.clone(),
      vec![Arc::new(Int64Array::from_iter_values(0..1024))],
    )
    .unwrap();
    let options = IpcWriteOptions::default()
      .try_with_compression(compression)
      .unwrap();
    FlightDataEncoderBuilder::new()
      .with_options(options)
      .with_schema(schema)
      .build(futures::stream::iter([Ok(batch)]))
      .try_collect()
      .await
      .unwrap()
  }

  /// The record batch message and the offset of its first non-empty buffer's prefix.
  fn batch_and_prefix(messages: &[FlightData]) -> (FlightData, usize) {
    let data = messages.last().unwrap().clone();
    let offset = root_as_message(&data.data_header)
      .unwrap()
      .header_as_record_batch()
      .unwrap()
      .buffers()
      .unwrap()
      .iter()
      .find(|b| b.length() > 0)
      .unwrap()
      .offset() as usize;
    (data, offset)
  }

  fn with_prefix(mut data: FlightData, offset: usize, length: i64) -> FlightData {
    let mut body = data.data_body.to_vec();
    body[offset..offset + PREFIX_BYTES].copy_from_slice(&length.to_le_bytes());
    data.data_body = body.into();
    data
  }

  #[tokio::test]
  async fn passes_compressed_and_uncompressed_batches_within_the_cap() {
    for compression in [
      None,
      Some(CompressionType::LZ4_FRAME),
      Some(CompressionType::ZSTD),
    ] {
      for data in encode(compression).await {
        check_decompressed_size(&data, MAX_DECOMPRESSED_MESSAGE_BYTES).unwrap();
      }
    }
  }

  #[tokio::test]
  async fn refuses_a_declared_length_over_the_cap() {
    let (data, offset) = batch_and_prefix(&encode(Some(CompressionType::LZ4_FRAME)).await);
    let crafted = with_prefix(data, offset, i64::MAX);
    let error = check_decompressed_size(&crafted, MAX_DECOMPRESSED_MESSAGE_BYTES).unwrap_err();
    assert!(error.to_string().contains("decompressed bytes"), "{error}");
  }

  #[tokio::test]
  async fn refuses_a_negative_declared_length() {
    let (data, offset) = batch_and_prefix(&encode(Some(CompressionType::ZSTD)).await);
    let crafted = with_prefix(data, offset, -2);
    let error = check_decompressed_size(&crafted, MAX_DECOMPRESSED_MESSAGE_BYTES).unwrap_err();
    assert!(error.to_string().contains("negative length"), "{error}");
  }

  #[tokio::test]
  async fn passes_a_buffer_marked_uncompressed() {
    let (data, offset) = batch_and_prefix(&encode(Some(CompressionType::LZ4_FRAME)).await);
    let marked = with_prefix(data, offset, -1);
    check_decompressed_size(&marked, MAX_DECOMPRESSED_MESSAGE_BYTES).unwrap();
  }

  #[tokio::test]
  async fn leaves_unparseable_messages_to_the_decoder() {
    let garbage = FlightData {
      data_header: vec![0xff; 16].into(),
      ..Default::default()
    };
    check_decompressed_size(&garbage, 0).unwrap();
  }

  #[tokio::test]
  async fn refuses_a_compressed_buffer_outside_the_body() {
    let (mut data, _) = batch_and_prefix(&encode(Some(CompressionType::LZ4_FRAME)).await);
    data.data_body = bytes::Bytes::new();
    let error = check_decompressed_size(&data, MAX_DECOMPRESSED_MESSAGE_BYTES).unwrap_err();
    assert!(
      error.to_string().contains("outside the message body"),
      "{error}"
    );
  }

  #[tokio::test]
  async fn checks_a_compressed_dictionary_batch() {
    use arrow_array::types::Int32Type;
    use arrow_array::{Array, DictionaryArray};

    let values: DictionaryArray<Int32Type> = (0..1024).map(|i| ["a", "b", "c"][i % 3]).collect();
    let schema = Arc::new(Schema::new(vec![Field::new(
      "d",
      values.data_type().clone(),
      false,
    )]));
    let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(values)]).unwrap();
    let options = IpcWriteOptions::default()
      .try_with_compression(Some(CompressionType::ZSTD))
      .unwrap();
    let messages: Vec<FlightData> = FlightDataEncoderBuilder::new()
      .with_options(options)
      .with_dictionary_handling(arrow_flight::encode::DictionaryHandling::Resend)
      .with_schema(schema)
      .build(futures::stream::iter([Ok(batch)]))
      .try_collect()
      .await
      .unwrap();
    let dictionary = messages
      .iter()
      .find(|data| {
        root_as_message(&data.data_header)
          .map(|m| m.header_type() == MessageHeader::DictionaryBatch)
          .unwrap_or(false)
      })
      .unwrap()
      .clone();
    check_decompressed_size(&dictionary, MAX_DECOMPRESSED_MESSAGE_BYTES).unwrap();

    let offset = root_as_message(&dictionary.data_header)
      .unwrap()
      .header_as_dictionary_batch()
      .unwrap()
      .data()
      .unwrap()
      .buffers()
      .unwrap()
      .iter()
      .find(|b| b.length() > 0)
      .unwrap()
      .offset() as usize;
    let crafted = with_prefix(dictionary, offset, i64::MAX);
    assert!(check_decompressed_size(&crafted, MAX_DECOMPRESSED_MESSAGE_BYTES).is_err());
  }

  #[tokio::test]
  async fn guarded_stream_yields_the_refusal() {
    let (data, offset) = batch_and_prefix(&encode(Some(CompressionType::LZ4_FRAME)).await);
    let crafted = with_prefix(data, offset, i64::MAX);
    let results: Vec<_> = guard_decompressed_size(futures::stream::iter([Ok(crafted)]))
      .collect()
      .await;
    assert!(matches!(results[0], Err(FlightError::ProtocolError(_))));
  }
}
