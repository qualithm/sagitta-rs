//! Per-RPC Flight metrics, emitted through the [`metrics`] facade.
//!
//! sagitta ships no recorder; these calls are no-ops until an embedder installs
//! a global `metrics` recorder (for example a Prometheus exporter), which then
//! collects them. See the "sagitta emits per-RPC metrics via the metrics
//! facade" decision.

use std::time::Instant;

use metrics::{counter, histogram};

/// Record the outcome of a Flight RPC: a request count, a duration observation,
/// and, when `ok` is false, an error count — all labeled by `method`.
pub(crate) fn record_rpc(method: &'static str, start: Instant, ok: bool) {
  counter!("sagitta_flight_rpc_requests_total", "method" => method).increment(1);
  histogram!("sagitta_flight_rpc_duration_seconds", "method" => method)
    .record(start.elapsed().as_secs_f64());
  if !ok {
    counter!("sagitta_flight_rpc_errors_total", "method" => method).increment(1);
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  use metrics_util::debugging::DebuggingRecorder;

  #[test]
  fn record_rpc_emits_request_duration_and_error_metrics() {
    let recorder = DebuggingRecorder::new();
    let snapshotter = recorder.snapshotter();
    metrics::with_local_recorder(&recorder, || {
      record_rpc("do_get", Instant::now(), true); // success
      record_rpc("do_put", Instant::now(), false); // failure
    });

    let names: Vec<String> = snapshotter
      .snapshot()
      .into_vec()
      .into_iter()
      .map(|(key, _unit, _desc, _value)| key.key().name().to_string())
      .collect();

    // Every RPC records a request count and a duration; only the failure also
    // records an error.
    assert_eq!(
      names
        .iter()
        .filter(|n| n.as_str() == "sagitta_flight_rpc_requests_total")
        .count(),
      2
    );
    assert_eq!(
      names
        .iter()
        .filter(|n| n.as_str() == "sagitta_flight_rpc_duration_seconds")
        .count(),
      2
    );
    assert_eq!(
      names
        .iter()
        .filter(|n| n.as_str() == "sagitta_flight_rpc_errors_total")
        .count(),
      1
    );
  }
}
