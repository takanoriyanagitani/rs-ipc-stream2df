use std::io;
use std::process::ExitCode;

use arrow_array::RecordBatch;
use clap::Parser;
use datafusion::dataframe::DataFrame;
use rs_ipc_stream2df::df::{LazyRows, bat2df};
use rs_ipc_stream2df::ipc::IpcStreamReader;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value_t = 1048576)]
    max_rows: usize,

    #[arg(short, long, default_value = "ipc_table")]
    tabname: String,

    #[arg(short, long, default_value = "SELECT * FROM ipc_table")]
    sql: String,
}

async fn sub() -> Result<(), io::Error> {
    let args = Args::parse();

    let i = io::stdin().lock();

    let proj: Option<Vec<usize>> = None;

    let irdr = IpcStreamReader::from_rdr(i, proj)?;
    let rbat: RecordBatch = irdr.into_batch(args.max_rows)?;

    let df: DataFrame = bat2df(rbat, &args.tabname, &args.sql).await?;
    let lr = LazyRows(df);

    lr.into_ipc_stream_stdout().await?;

    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    match sub().await {
        Ok(_) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{e}");
            ExitCode::FAILURE
        }
    }
}
