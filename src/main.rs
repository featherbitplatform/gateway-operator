//! CLI: `run` (controller + webhook + metrics), `crds` (print CRD YAML), `version`.
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "featherbit-operator",
    about = "Kubernetes operator for the featherbit API gateway"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run the controller, admission webhook and metrics server.
    Run(RunArgs),
    /// Print the CustomResourceDefinitions as YAML.
    Crds,
    /// Print the version.
    Version,
}

#[derive(clap::Args, Clone)]
struct RunArgs {
    /// Webhook TLS certificate (PEM).
    #[arg(long, default_value = "/etc/webhook/tls/tls.crt")]
    tls_cert: std::path::PathBuf,
    /// Webhook TLS private key (PEM).
    #[arg(long, default_value = "/etc/webhook/tls/tls.key")]
    tls_key: std::path::PathBuf,
    /// Webhook listen address.
    #[arg(long, default_value = "0.0.0.0:9443")]
    webhook_addr: std::net::SocketAddr,
    /// Metrics + health listen address.
    #[arg(long, default_value = "0.0.0.0:8080")]
    metrics_addr: std::net::SocketAddr,
    /// Log format: text | json.
    #[arg(long, default_value = "text")]
    log_format: String,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::Version => println!("featherbit-operator {}", env!("CARGO_PKG_VERSION")),
        Command::Crds => print!("{}", featherbit_operator::crd::all_crds_yaml()),
        Command::Run(args) => {
            if let Err(e) = featherbit_operator::run::run(featherbit_operator::run::RunConfig {
                tls_cert: args.tls_cert,
                tls_key: args.tls_key,
                webhook_addr: args.webhook_addr,
                metrics_addr: args.metrics_addr,
                log_format: args.log_format,
            })
            .await
            {
                eprintln!("{e:#}");
                std::process::exit(1);
            }
        }
    }
}
