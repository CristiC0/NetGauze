// Copyright (C) 2022-present The NetGauze Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use netgauze_bmp_pkt::BmpMessage;
use netgauze_bmp_pkt::codec::BmpCodecDecoderError;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

pub mod actor;
pub mod handle;
pub mod server;
pub mod supervisor;
pub mod transport;

pub type ActorId = u32;
pub type SubscriberId = u32;
pub type BmpRequest = (AddrInfo, BmpMessage);

pub type BmpSender = async_channel::Sender<Arc<BmpRequest>>;
pub type BmpReceiver = async_channel::Receiver<Arc<BmpRequest>>;

pub fn create_bmp_channel(buffer_size: usize) -> (BmpSender, BmpReceiver) {
    async_channel::bounded(buffer_size)
}

#[derive(Debug, Clone)]
pub struct Subscription {
    actor_id: ActorId,
    id: SubscriberId,
}

impl Subscription {
    pub const fn new(actor_id: ActorId, id: SubscriberId) -> Self {
        Self { actor_id, id }
    }

    pub const fn actor_id(&self) -> ActorId {
        self.actor_id
    }

    pub const fn id(&self) -> SubscriberId {
        self.id
    }
}
impl Display for Subscription {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Subscription {{ actor_id: {}, id: {} }}",
            self.actor_id, self.id
        )
    }
}

/// Capture the address of both sides of a socket
#[derive(Copy, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Debug, Serialize, Deserialize)]
pub struct AddrInfo {
    local_socket: SocketAddr,
    remote_socket: SocketAddr,
}

impl AddrInfo {
    pub const fn new(local_socket: SocketAddr, remote_socket: SocketAddr) -> Self {
        Self {
            local_socket,
            remote_socket,
        }
    }

    pub const fn local_socket(&self) -> SocketAddr {
        self.local_socket
    }

    pub const fn remote_socket(&self) -> SocketAddr {
        self.remote_socket
    }
}

/// Associate a value with a tag
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaggedData<T, V> {
    tag: T,
    value: V,
}

impl<T: Copy, V> TaggedData<T, V> {
    pub const fn new(tag: T, value: V) -> Self {
        Self { tag, value }
    }

    pub const fn tag(&self) -> T {
        self.tag
    }

    pub const fn value(&self) -> &V {
        &self.value
    }
}

impl Display for TaggedData<AddrInfo, BmpCodecDecoderError> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}

impl std::error::Error for TaggedData<AddrInfo, BmpCodecDecoderError> {}

/// TCP keepalive parameters for BMP listener sockets, applied to the listener
/// and inherited by every accepted connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TcpKeepaliveConfig {
    /// Seconds a connection must be idle before the first keepalive probe is
    /// sent (`TCP_KEEPIDLE`)
    pub idle_secs: u64,
    /// Seconds between keepalive probes (`TCP_KEEPINTVL`)
    pub interval_secs: u64,
    /// Number of unanswered probes before the connection is dropped
    /// (`TCP_KEEPCNT`)
    pub retries: u32,
}

impl Default for TcpKeepaliveConfig {
    fn default() -> Self {
        Self {
            idle_secs: 120,
            interval_secs: 30,
            retries: 5,
        }
    }
}

pub fn default_tcp_keepalive() -> Option<TcpKeepaliveConfig> {
    Some(TcpKeepaliveConfig::default())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TcpKeepaliveConfigError {
    ZeroValue(&'static str),
}

impl Display for TcpKeepaliveConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroValue(field) => {
                write!(f, "TCP keepalive `{field}` must be greater than 0")
            }
        }
    }
}

impl std::error::Error for TcpKeepaliveConfigError {}

impl TcpKeepaliveConfig {
    pub fn validate(&self) -> Result<(), TcpKeepaliveConfigError> {
        if self.idle_secs == 0 {
            return Err(TcpKeepaliveConfigError::ZeroValue("idle_secs"));
        }
        if self.interval_secs == 0 {
            return Err(TcpKeepaliveConfigError::ZeroValue("interval_secs"));
        }
        if self.retries == 0 {
            return Err(TcpKeepaliveConfigError::ZeroValue("retries"));
        }
        Ok(())
    }
}

impl From<TcpKeepaliveConfig> for socket2::TcpKeepalive {
    fn from(config: TcpKeepaliveConfig) -> Self {
        let keepalive =
            socket2::TcpKeepalive::new().with_time(Duration::from_secs(config.idle_secs));

        // `with_interval`/`with_retries` are only available on a subset of
        // platforms, so they are guarded by `socket2`'s own target list.
        #[cfg(any(
            target_os = "android",
            target_os = "dragonfly",
            target_os = "freebsd",
            target_os = "fuchsia",
            target_os = "illumos",
            target_os = "ios",
            target_os = "visionos",
            target_os = "linux",
            target_os = "macos",
            target_os = "netbsd",
            target_os = "tvos",
            target_os = "watchos",
            target_os = "windows",
            target_os = "cygwin",
            all(target_os = "wasi", not(target_env = "p1")),
        ))]
        let keepalive = keepalive
            .with_interval(Duration::from_secs(config.interval_secs))
            .with_retries(config.retries);
        keepalive
    }
}

/// Enable socket reuse and bind to a device or a VRF on selected platforms for
/// TCP. Binding to device or VRF is supported on: MacOS and Linux.
///
/// When `keepalive` is `Some`, TCP keepalive is enabled on the listener and
/// inherited by every accepted connection, so dead peers get detected. `None`
/// disables it.
pub fn new_tcp_reuse_port(
    local_addr: SocketAddr,
    device: Option<String>,
    backlog: i32,
    keepalive: Option<TcpKeepaliveConfig>,
) -> io::Result<tokio::net::TcpListener> {
    let tcp_sock = socket2::Socket::new(
        if local_addr.is_ipv4() {
            socket2::Domain::IPV4
        } else {
            socket2::Domain::IPV6
        },
        socket2::Type::STREAM,
        None,
    )?;
    tcp_sock.set_reuse_address(true)?;
    #[cfg(all(unix, not(any(target_os = "solaris", target_os = "illumos"))))]
    tcp_sock.set_reuse_port(true)?;
    #[cfg(unix)]
    tcp_sock.set_cloexec(true)?;
    tcp_sock.set_nonblocking(true)?;
    if let Some(keepalive) = keepalive {
        keepalive
            .validate()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        tcp_sock
            .set_tcp_keepalive(&keepalive.into())
            .map_err(|e| io::Error::new(e.kind(), format!("failed to apply TCP keepalive: {e}")))?;
    }

    #[cfg(any(
        target_os = "ios",
        target_os = "macos",
        target_os = "tvos",
        target_os = "watchos",
        target_os = "android",
        target_os = "fuchsia",
        target_os = "linux"
    ))]
    if let Some(name) = device {
        #[cfg(any(
            target_os = "ios",
            target_os = "macos",
            target_os = "tvos",
            target_os = "watchos",
        ))]
        {
            let c_str = std::ffi::CString::new(name)?;
            let c_index = unsafe { libc::if_nametoindex(c_str.as_ptr() as *const libc::c_char) };
            let index = std::num::NonZeroU32::new(c_index as u32);
            if local_addr.is_ipv4() {
                tcp_sock.bind_device_by_index_v4(index)?;
            } else {
                tcp_sock.bind_device_by_index_v6(index)?;
            }
        }
        #[cfg(any(target_os = "android", target_os = "fuchsia", target_os = "linux"))]
        tcp_sock.bind_device(Some(name.as_bytes()))?
    }

    tcp_sock.bind(&socket2::SockAddr::from(local_addr))?;
    tcp_sock.listen(backlog)?;
    let tcp_sock: std::net::TcpListener = tcp_sock.into();
    tokio::net::TcpListener::from_std(tcp_sock)
}

#[cfg(test)]
mod tests;
