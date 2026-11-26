//! Target-neutral host platform boundary isolating OS-specific FFI, file locking,
//! directory barriers, filesystem stream checks, and Windows named-pipe primitives.

#[cfg(not(windows))]
mod other;
#[cfg(windows)]
mod windows;

#[cfg(not(windows))]
pub use other::{
    canonical_path_workspace_max, check_directory_streams, check_regular_file_streams,
    open_catalogue_owner, open_catalogue_writer, open_exclusive_owner, open_read_only_exclusive,
    prepare_quarantined_lock, sync_dir, sync_file, validate_regular_metadata,
    CHECKS_ALTERNATE_STREAMS,
};
#[cfg(windows)]
pub use windows::{
    canonical_path_workspace_max, check_directory_streams, check_regular_file_streams,
    create_inbound_pipe, disconnect_pipe, file_id, is_pipe_handle, open_catalogue_owner,
    open_catalogue_writer, open_exclusive_owner, open_held_custody_options, open_overlapped_pipe,
    open_read_only_exclusive, open_signal_pipe_writer, pipe_server_pid, prepare_quarantined_lock,
    sync_dir, sync_file, validate_default_stream_leaf, validate_regular_metadata,
    SecuredPipeListener, CHECKS_ALTERNATE_STREAMS,
};
