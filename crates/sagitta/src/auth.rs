//! Authentication types and utilities.

use std::collections::HashMap;
use std::sync::Arc;

/// User access level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessLevel {
    /// Read-only access.
    ReadOnly,
    /// Full read/write access.
    FullAccess,
}

/// Authenticated user information.
#[derive(Debug, Clone)]
pub struct User {
    /// Username.
    pub username: String,
    /// Access level.
    pub access: AccessLevel,
}

impl User {
    /// Create a new user.
    pub fn new(username: impl Into<String>, access: AccessLevel) -> Self {
        Self {
            username: username.into(),
            access,
        }
    }

    /// Check if user has write access.
    pub fn can_write(&self) -> bool {
        self.access == AccessLevel::FullAccess
    }
}

/// Bearer token for authenticated sessions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AuthToken(String);

impl AuthToken {
    /// Create a new auth token.
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    /// Get the token string.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Convert to bytes for wire format.
    pub fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl std::fmt::Display for AuthToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Pluggable authentication backend.
///
/// Implement this trait to integrate custom identity providers (LDAP,
/// OAuth, database-backed, etc.).
pub trait UserStore: Send + Sync {
    /// Validate credentials and return user if valid.
    fn authenticate(&self, username: &str, password: &str) -> Option<User>;

    /// Look up user by token.
    fn user_for_token(&self, token: &AuthToken) -> Option<User>;

    /// Create and store a token for an authenticated user.
    fn create_token(&self, user: &User) -> AuthToken;
}

/// In-memory [`UserStore`] for testing and development.
///
/// All credentials and tokens are held in memory and lost on restart.
/// Tokens expire after `token_ttl_secs` seconds (default 3600).
#[derive(Debug)]
pub struct InMemoryUserStore {
    users: HashMap<String, (String, AccessLevel)>,
    tokens: std::sync::RwLock<HashMap<AuthToken, (User, std::time::Instant)>>,
    /// Token time-to-live in seconds. Zero means tokens never expire.
    token_ttl_secs: u64,
}

impl Default for InMemoryUserStore {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryUserStore {
    /// Create a new empty user store with a 1-hour token TTL.
    pub fn new() -> Self {
        Self {
            users: HashMap::new(),
            tokens: std::sync::RwLock::new(HashMap::new()),
            token_ttl_secs: 3600,
        }
    }

    /// Create a new empty user store with a custom token TTL.
    ///
    /// Pass `0` to disable token expiry.
    pub fn with_token_ttl(token_ttl_secs: u64) -> Self {
        Self {
            users: HashMap::new(),
            tokens: std::sync::RwLock::new(HashMap::new()),
            token_ttl_secs,
        }
    }

    /// Create user store with test users (admin and reader).
    pub fn with_test_users() -> Self {
        let mut users = HashMap::new();
        users.insert(
            "admin".to_string(),
            ("admin123".to_string(), AccessLevel::FullAccess),
        );
        users.insert(
            "reader".to_string(),
            ("reader123".to_string(), AccessLevel::ReadOnly),
        );
        Self {
            users,
            tokens: std::sync::RwLock::new(HashMap::new()),
            token_ttl_secs: 3600,
        }
    }

    /// Add a user to the store.
    pub fn add_user(&mut self, username: &str, password: &str, access: AccessLevel) {
        self.users
            .insert(username.to_string(), (password.to_string(), access));
    }

    /// Remove expired tokens from the store.
    fn evict_expired(&self) {
        if self.token_ttl_secs == 0 {
            return;
        }
        let now = std::time::Instant::now();
        let ttl = std::time::Duration::from_secs(self.token_ttl_secs);
        let mut tokens = self.tokens.write().unwrap();
        tokens.retain(|_, (_, created_at)| now.duration_since(*created_at) < ttl);
    }

    /// Generate and store a token for a user.
    pub fn create_token(&self, user: &User) -> AuthToken {
        // Simple token: base64(username:timestamp:random)
        let token_data = format!(
            "{}:{}:{}",
            user.username,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
            rand_simple()
        );
        let token = AuthToken::new(base64_encode(&token_data));

        let mut tokens = self.tokens.write().unwrap();
        tokens.insert(token.clone(), (user.clone(), std::time::Instant::now()));

        token
    }
}

impl UserStore for InMemoryUserStore {
    fn authenticate(&self, username: &str, password: &str) -> Option<User> {
        self.users
            .get(username)
            .and_then(|(stored_password, access)| {
                if stored_password == password {
                    Some(User::new(username, *access))
                } else {
                    None
                }
            })
    }

    fn user_for_token(&self, token: &AuthToken) -> Option<User> {
        self.evict_expired();
        let tokens = self.tokens.read().unwrap();
        if self.token_ttl_secs == 0 {
            tokens.get(token).map(|(user, _)| user.clone())
        } else {
            let ttl = std::time::Duration::from_secs(self.token_ttl_secs);
            tokens.get(token).and_then(|(user, created_at)| {
                if std::time::Instant::now().duration_since(*created_at) < ttl {
                    Some(user.clone())
                } else {
                    None
                }
            })
        }
    }

    fn create_token(&self, user: &User) -> AuthToken {
        InMemoryUserStore::create_token(self, user)
    }
}

impl UserStore for Arc<InMemoryUserStore> {
    fn authenticate(&self, username: &str, password: &str) -> Option<User> {
        self.as_ref().authenticate(username, password)
    }

    fn user_for_token(&self, token: &AuthToken) -> Option<User> {
        self.as_ref().user_for_token(token)
    }

    fn create_token(&self, user: &User) -> AuthToken {
        self.as_ref().create_token(user)
    }
}

/// Simple base64 encoding without external dependency.
fn base64_encode(input: &str) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let bytes = input.as_bytes();
    let mut result = String::new();

    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }

    result
}

/// Simple random number generator (xorshift).
fn rand_simple() -> u64 {
    use std::cell::Cell;
    use std::time::{SystemTime, UNIX_EPOCH};

    thread_local! {
        static STATE: Cell<u64> = Cell::new(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64
        );
    }

    STATE.with(|state| {
        let mut x = state.get();
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        state.set(x);
        x
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_authenticate_valid_user() {
        let store = InMemoryUserStore::with_test_users();
        let user = store.authenticate("admin", "admin123");
        assert!(user.is_some());
        let user = user.unwrap();
        assert_eq!(user.username, "admin");
        assert_eq!(user.access, AccessLevel::FullAccess);
    }

    #[test]
    fn test_authenticate_invalid_password() {
        let store = InMemoryUserStore::with_test_users();
        let user = store.authenticate("admin", "wrong");
        assert!(user.is_none());
    }

    #[test]
    fn test_authenticate_unknown_user() {
        let store = InMemoryUserStore::with_test_users();
        let user = store.authenticate("unknown", "password");
        assert!(user.is_none());
    }

    #[test]
    fn test_token_creation_and_lookup() {
        let store = InMemoryUserStore::with_test_users();
        let user = store.authenticate("reader", "reader123").unwrap();
        let token = store.create_token(&user);

        let looked_up = store.user_for_token(&token);
        assert!(looked_up.is_some());
        assert_eq!(looked_up.unwrap().username, "reader");
    }

    #[test]
    fn test_reader_cannot_write() {
        let store = InMemoryUserStore::with_test_users();
        let user = store.authenticate("reader", "reader123").unwrap();
        assert!(!user.can_write());
    }

    #[test]
    fn test_admin_can_write() {
        let store = InMemoryUserStore::with_test_users();
        let user = store.authenticate("admin", "admin123").unwrap();
        assert!(user.can_write());
    }
}
