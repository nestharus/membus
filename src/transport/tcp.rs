use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinSet;

use crate::transport::connection::serve_connection;
use crate::transport::{Endpoint, ServerContext, ServerHandle};
use crate::types::MembusError;

/// Bind a TCP listener and run an async accept loop with `TCP_NODELAY`.
pub async fn spawn_tcp_listener(
    addr: SocketAddr,
    ctx: ServerContext,
) -> Result<ServerHandle, MembusError> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let active_connections = Arc::clone(&ctx.active_connections);

    let accept_task = tokio::spawn(async move {
        let mut conn_tasks: JoinSet<()> = JoinSet::new();
        loop {
            while conn_tasks.try_join_next().is_some() {}
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok((stream, _peer)) => {
                            let _ = stream.set_nodelay(true);
                            let conn_ctx = ctx.clone();
                            let conn_shutdown = shutdown_rx.clone();
                            conn_tasks.spawn(serve_connection(stream, conn_ctx, conn_shutdown));
                        }
                        Err(_) => {
                            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                        }
                    }
                }
            }
        }
        // Drain active connection tasks cleanly before listener shutdown completes.
        while conn_tasks.join_next().await.is_some() {}
    });

    Ok(ServerHandle::new(
        Endpoint::Tcp(local_addr),
        shutdown_tx,
        accept_task,
        active_connections,
    ))
}

/// Dial a TCP server endpoint with `TCP_NODELAY` enabled.
pub async fn connect_tcp(addr: SocketAddr) -> Result<TcpStream, MembusError> {
    let stream = TcpStream::connect(addr).await?;
    let _ = stream.set_nodelay(true);
    Ok(stream)
}
