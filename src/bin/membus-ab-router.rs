use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = match membus::router::parse_router_args(args) {
        Ok(cmd) => cmd,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(2);
        }
    };

    if let Err(err) = membus::router::run_router_cli(command).await {
        eprintln!("error: {err}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
