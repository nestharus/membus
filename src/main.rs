use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let command = match membus::cli::parse_args(args) {
        Ok(cmd) => cmd,
        Err(err) => {
            eprintln!("{err}");
            return ExitCode::from(2);
        }
    };

    if let Err(err) = membus::cli::execute(command).await {
        eprintln!("error: {err}");
        return ExitCode::FAILURE;
    }

    ExitCode::SUCCESS
}
