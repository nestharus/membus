//! Binary entrypoint for the `membus` resident daemon and administrative CLI.
mod cli;

fn main() -> std::io::Result<()> {
    cli::run()
}
