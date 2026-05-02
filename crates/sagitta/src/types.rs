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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_flight::FlightDescriptor;

    #[test]
    fn test_data_path_new_and_segments() {
        let path = DataPath::new(vec!["a".to_string(), "b".to_string()]);
        assert_eq!(path.segments(), &["a", "b"]);
    }

    #[test]
    fn test_data_path_display() {
        let path = DataPath::from(vec!["schema", "table"]);
        assert_eq!(path.display(), "schema/table");
    }

    #[test]
    fn test_data_path_display_single_segment() {
        let path = DataPath::from(vec!["mytable"]);
        assert_eq!(path.display(), "mytable");
    }

    #[test]
    fn test_data_path_to_uri() {
        let path = DataPath::from(vec!["schema", "table"]);
        assert_eq!(path.to_uri("file:///data"), "file:///data/schema/table");
    }

    #[test]
    fn test_data_path_to_uri_trailing_slash() {
        let path = DataPath::from(vec!["schema", "table"]);
        assert_eq!(path.to_uri("file:///data/"), "file:///data/schema/table");
    }

    #[test]
    fn test_data_path_from_string_vec() {
        let path = DataPath::from(vec!["a".to_string(), "b".to_string()]);
        assert_eq!(path.segments(), &["a", "b"]);
    }

    #[test]
    fn test_data_path_equality() {
        let p1 = DataPath::from(vec!["a", "b"]);
        let p2 = DataPath::from(vec!["a", "b"]);
        assert_eq!(p1, p2);
    }

    #[test]
    fn test_data_path_hash() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        let path = DataPath::from(vec!["a", "b"]);
        map.insert(path.clone(), 42);
        assert_eq!(map[&path], 42);
    }

    #[test]
    fn test_flight_descriptor_path_segments_non_empty() {
        let descriptor = FlightDescriptor::new_path(vec!["a".to_string(), "b".to_string()]);
        let segments = descriptor.path_segments();
        assert!(segments.is_some());
        assert_eq!(segments.unwrap(), vec!["a", "b"]);
    }

    #[test]
    fn test_flight_descriptor_path_segments_empty() {
        let descriptor = FlightDescriptor::new_cmd("SELECT 1");
        let segments = descriptor.path_segments();
        assert!(segments.is_none());
    }

    #[test]
    fn test_flight_descriptor_command_bytes_non_empty() {
        let descriptor = FlightDescriptor::new_cmd("SELECT 1");
        let cmd = descriptor.command_bytes();
        assert!(cmd.is_some());
        assert_eq!(cmd.unwrap().as_ref(), b"SELECT 1");
    }

    #[test]
    fn test_flight_descriptor_command_bytes_empty_for_path() {
        let descriptor = FlightDescriptor::new_path(vec!["a".to_string()]);
        let cmd = descriptor.command_bytes();
        assert!(cmd.is_none());
    }

    #[test]
    fn test_flight_descriptor_to_data_path() {
        let descriptor = FlightDescriptor::new_path(vec!["a".to_string(), "b".to_string()]);
        let path = descriptor.to_data_path();
        assert!(path.is_some());
        assert_eq!(path.unwrap().segments(), &["a", "b"]);
    }

    #[test]
    fn test_flight_descriptor_to_data_path_cmd_returns_none() {
        let descriptor = FlightDescriptor::new_cmd("SELECT 1");
        let path = descriptor.to_data_path();
        assert!(path.is_none());
    }
}
