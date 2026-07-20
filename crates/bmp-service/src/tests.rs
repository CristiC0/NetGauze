// Copyright (C) 2026-present The NetGauze Authors.
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

use super::*;

fn create_test_keepalive() -> TcpKeepaliveConfig {
    TcpKeepaliveConfig {
        idle_secs: 42,
        interval_secs: 7,
        retries: 3,
    }
}

#[tokio::test]
async fn test_tcp_keepalive_set_on_listener() {
    let cfg = create_test_keepalive();
    let listener = new_tcp_reuse_port(SocketAddr::from(([127, 0, 0, 1], 0)), None, 8, Some(cfg))
        .expect("failed to bind listener");

    let sock = socket2::SockRef::from(&listener);
    assert!(sock.keepalive().expect("failed to read SO_KEEPALIVE"));
    assert_eq!(
        sock.tcp_keepalive_time().unwrap(),
        Duration::from_secs(cfg.idle_secs)
    );
    assert_eq!(
        sock.tcp_keepalive_interval().unwrap(),
        Duration::from_secs(cfg.interval_secs)
    );
    assert_eq!(sock.tcp_keepalive_retries().unwrap(), cfg.retries);
}

#[tokio::test]
async fn test_tcp_keepalive_disabled_on_listener() {
    let listener = new_tcp_reuse_port(SocketAddr::from(([127, 0, 0, 1], 0)), None, 8, None)
        .expect("failed to bind listener");

    let sock = socket2::SockRef::from(&listener);
    assert!(!sock.keepalive().expect("failed to read SO_KEEPALIVE"));
}

#[tokio::test]
async fn test_tcp_keepalive_inherited_by_accepted_connection() {
    let cfg = create_test_keepalive();
    let listener = new_tcp_reuse_port(SocketAddr::from(([127, 0, 0, 1], 0)), None, 8, Some(cfg))
        .expect("failed to bind listener");
    let local_addr = listener.local_addr().expect("failed to read local addr");

    let _client = tokio::net::TcpStream::connect(local_addr)
        .await
        .expect("failed to connect");
    let (accepted, _) = listener.accept().await.expect("failed to accept");

    let sock = socket2::SockRef::from(&accepted);
    assert!(sock.keepalive().expect("failed to read SO_KEEPALIVE"));
    assert_eq!(
        sock.tcp_keepalive_time().unwrap(),
        Duration::from_secs(cfg.idle_secs)
    );
    assert_eq!(
        sock.tcp_keepalive_interval().unwrap(),
        Duration::from_secs(cfg.interval_secs)
    );
    assert_eq!(sock.tcp_keepalive_retries().unwrap(), cfg.retries);
}

#[test]
fn test_tcp_keepalive_config_serde_requires_all_fields() {
    let full: TcpKeepaliveConfig =
        serde_json::from_str(r#"{"idle_secs": 30, "interval_secs": 10, "retries": 4}"#).unwrap();
    assert_eq!(
        full,
        TcpKeepaliveConfig {
            idle_secs: 30,
            interval_secs: 10,
            retries: 4,
        }
    );

    assert!(serde_json::from_str::<TcpKeepaliveConfig>("{}").is_err());
    assert!(serde_json::from_str::<TcpKeepaliveConfig>(r#"{"idle_secs": 30}"#).is_err());
}

#[test]
fn test_tcp_keepalive_config_optional_key() {
    #[derive(Deserialize)]
    struct Wrapper {
        #[serde(default = "default_tcp_keepalive")]
        keepalive: Option<TcpKeepaliveConfig>,
    }
    let omitted: Wrapper = serde_json::from_str("{}").unwrap();
    assert_eq!(omitted.keepalive, Some(TcpKeepaliveConfig::default()));
    let disabled: Wrapper = serde_json::from_str(r#"{"keepalive": null}"#).unwrap();
    assert_eq!(disabled.keepalive, None);
}

#[test]
fn test_tcp_keepalive_config_validate_rejects_zero() {
    assert!(TcpKeepaliveConfig::default().validate().is_ok());

    let zero_idle = TcpKeepaliveConfig {
        idle_secs: 0,
        ..Default::default()
    };
    assert_eq!(
        zero_idle.validate(),
        Err(TcpKeepaliveConfigError::ZeroValue("idle_secs"))
    );

    let zero_interval = TcpKeepaliveConfig {
        interval_secs: 0,
        ..Default::default()
    };
    assert_eq!(
        zero_interval.validate(),
        Err(TcpKeepaliveConfigError::ZeroValue("interval_secs"))
    );

    let zero_retries = TcpKeepaliveConfig {
        retries: 0,
        ..Default::default()
    };
    assert_eq!(
        zero_retries.validate(),
        Err(TcpKeepaliveConfigError::ZeroValue("retries"))
    );
}

#[tokio::test]
async fn test_new_tcp_reuse_port_rejects_invalid_keepalive() {
    let cfg = TcpKeepaliveConfig {
        idle_secs: 0,
        ..Default::default()
    };
    let err = new_tcp_reuse_port(SocketAddr::from(([127, 0, 0, 1], 0)), None, 8, Some(cfg))
        .expect_err("zero idle_secs must be rejected before bind");
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert!(err.to_string().contains("idle_secs"));
}
