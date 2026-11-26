//! Local named-pipe transport I/O and shared endpoint validation.
pub mod local_pipe;
#[cfg(windows)]
pub mod pipe_io;
