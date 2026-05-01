//! Core types for Sagitta.

use arrow_flight::FlightDescriptor;
use bytes::Bytes;

/// A data path represented as a vector of path segments.
///
/// For example, `["test", "integers"]` represents a dataset at path `test/integers`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DataPath(Vec<String>);

impl DataPath {
    /// Create a new data path from segments.
    pub fn new(segments: Vec<String>) -> Self {
        Self(segments)
    }

    /// Get the path segments.
    pub fn segments(&self) -> &[String] {
        &self.0
    }

    /// Convert to a display string (e.g., "test/integers").
    pub fn display(&self) -> String {
        self.0.join("/")
    }

    /// Convert to a URI by appending the path to `base`.
    ///
    /// A `/` separator is inserted between `base` and the path segments if
    /// `base` does not already end with one.
    ///
    /// # Examples
    ///
    /// ```
    /// # use sagitta::DataPath;
    /// let path = DataPath::from(vec!["schema", "table"]);
    /// assert_eq!(path.to_uri("file:///data"), "file:///data/schema/table");
    /// ```
    pub fn to_uri(&self, base: &str) -> String {
        let base = base.trim_end_matches('/');
        format!("{}/{}", base, self.0.join("/"))
    }
}

impl From<Vec<String>> for DataPath {
    fn from(segments: Vec<String>) -> Self {
        Self::new(segments)
    }
}

impl From<Vec<&str>> for DataPath {
    fn from(segments: Vec<&str>) -> Self {
        Self::new(segments.into_iter().map(String::from).collect())
    }
}

/// Extension trait for `FlightDescriptor` providing helper methods.
pub trait FlightDescriptorExt {
    /// Extract path segments from a PATH-type descriptor.
    fn path_segments(&self) -> Option<Vec<String>>;

    /// Extract command bytes from a CMD-type descriptor.
    fn command_bytes(&self) -> Option<&Bytes>;

    /// Convert to a `DataPath` if this is a PATH descriptor.
    fn to_data_path(&self) -> Option<DataPath>;
}

impl FlightDescriptorExt for FlightDescriptor {
    fn path_segments(&self) -> Option<Vec<String>> {
        if self.path.is_empty() {
            None
        } else {
            Some(self.path.clone())
        }
    }

    fn command_bytes(&self) -> Option<&Bytes> {
        if self.cmd.is_empty() {
            None
        } else {
            Some(&self.cmd)
        }
    }

    fn to_data_path(&self) -> Option<DataPath> {
        self.path_segments().map(DataPath::new)
    }
}
