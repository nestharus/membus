use std::sync::Arc;

use membus::{
    extract_tenant_id, tenant_namespace, tenant_token_binding, Ack, AuthConfig, AuthStore, Client,
    MembusError, MembusServer, Permission, RocksDbStorage, ServerConfig, SignalBroker,
    SignalConfig, SignalPublisher, SignalRing, StorageConfig, StorageEngine,
};

#[test]
fn tenant_namespace_and_prefix_isolation_validation() {
    assert_eq!(tenant_namespace("acme", "work").unwrap(), "t:acme:work");
    assert_eq!(extract_tenant_id("t:acme:work"), Some("acme"));
    assert_eq!(extract_tenant_id("t:acme:bad:ws"), None);

    for bad_tenant in ["", "a:b", "a*b", "../x", "-bad", "has space"] {
        assert!(tenant_namespace(bad_tenant, "ws").is_err(), "{bad_tenant:?}");
    }
    for bad_ws in ["", "a:b", "a*b", "a..b", "trail.", ".lead"] {
        assert!(tenant_namespace("acme", bad_ws).is_err(), "{bad_ws:?}");
    }
    // Reject short or whitespace-padded tokens.
    assert!(tenant_token_binding("short", "p1", "acme", vec![Permission::Read]).is_err());
    assert!(tenant_token_binding(" 0123456789abcdef", "p1", "acme", vec![Permission::Read]).is_err());
}

#[tokio::test]
async fn dynamic_auth_store_enforces_tenant_isolation_and_live_rotation_without_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let signal_cfg = SignalConfig::default();
    let ring = Arc::new(SignalRing::new(signal_cfg));
    let storage = Arc::new(
        RocksDbStorage::open(
            dir.path(),
            StorageConfig::default(),
            Arc::clone(&ring) as Arc<dyn SignalPublisher>,
        )
        .expect("open storage"),
    );
    let broker = Arc::new(SignalBroker::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        ring,
        signal_cfg,
    ));

    let tok_alpha = "alpha-secret-token-0123456789abcdef";
    let alpha_binding = tenant_token_binding(
        tok_alpha,
        "tenant:alpha",
        "alpha",
        vec![Permission::Read, Permission::Write],
    )
    .expect("alpha binding");

    let auth_store = Arc::new(AuthStore::new(AuthConfig::enforced(vec![alpha_binding])));
    let server = MembusServer::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        broker,
        ServerConfig::default(),
    )
    .with_auth_store(Arc::clone(&auth_store));

    let bound = server
        .bind_tcp("127.0.0.1:0".parse().expect("addr"))
        .await
        .expect("bind tcp");

    // 1. Tenant `alpha` can write to `t:alpha:ws1` but CANNOT touch `t:alpha2:ws1` (no prefix bleed!).
    let c_alpha = Client::connect(bound.endpoint()).await.expect("connect");
    assert_eq!(c_alpha.authenticate(tok_alpha).await.unwrap(), "tenant:alpha");

    let ns_alpha = tenant_namespace("alpha", "ws1").unwrap();
    let ns_alpha2 = tenant_namespace("alpha2", "ws1").unwrap();
    c_alpha
        .put(&ns_alpha, b"k1", b"v1", Ack::Flush)
        .await
        .expect("alpha writes own namespace");
    let err_bleed = c_alpha
        .put(&ns_alpha2, b"k1", b"stolen", Ack::Flush)
        .await
        .expect_err("alpha must not access alpha2");
    assert!(matches!(err_bleed, MembusError::Unauthorized(_)));

    // 2. Live-provision tenant `beta` WITHOUT restarting the server.
    let tok_beta = "beta-secret-token-fedcba9876543210";
    let beta_binding = tenant_token_binding(
        tok_beta,
        "tenant:beta",
        "beta",
        vec![Permission::Read, Permission::Write],
    )
    .expect("beta binding");
    auth_store.upsert_token(beta_binding).expect("upsert beta");

    let c_beta = Client::connect(bound.endpoint()).await.expect("connect beta");
    assert_eq!(c_beta.authenticate(tok_beta).await.unwrap(), "tenant:beta");
    let ns_beta = tenant_namespace("beta", "ws1").unwrap();
    c_beta
        .put(&ns_beta, b"k1", b"vbeta", Ack::Flush)
        .await
        .expect("beta writes own namespace");
    assert!(matches!(
        c_beta.get(&ns_alpha, b"k1").await,
        Err(MembusError::Unauthorized(_))
    ));

    // 3. Instant revocation prevents any new auth with revoked token.
    assert!(auth_store.revoke_token(tok_beta));
    assert!(!auth_store.revoke_token(tok_beta));
    let c_beta2 = Client::connect(bound.endpoint()).await.expect("connect beta2");
    assert!(matches!(
        c_beta2.authenticate(tok_beta).await,
        Err(MembusError::Unauthorized(_))
    ));

    bound.shutdown().await;
}
