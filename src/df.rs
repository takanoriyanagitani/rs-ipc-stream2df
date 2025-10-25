#![forbid(clippy::unwrap_used)]
use std::io;

use io::BufWriter;
use io::Write;

use arrow_schema::SchemaRef;

use arrow_array::RecordBatch;

use futures::stream::StreamExt;
use futures::stream::TryStreamExt;

use datafusion::dataframe::DataFrame;

use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::context::SessionContext;

use arrow_ipc::writer::StreamWriter;

pub struct LazyRows(pub DataFrame);

impl LazyRows {
    pub async fn into_batch_stream(self) -> Result<SendableRecordBatchStream, io::Error> {
        self.0.execute_stream().await.map_err(io::Error::other)
    }

    pub fn to_schema(&self) -> SchemaRef {
        self.0.schema().inner().clone()
    }
}

impl LazyRows {
    pub async fn write_ipc<W>(self, wtr: StreamWriter<BufWriter<W>>) -> Result<(), io::Error>
    where
        W: Write,
    {
        let strm = self.into_batch_stream().await?;
        let mapd = strm.map(|r| r.map_err(io::Error::other));
        let mut wtr = mapd
            .try_fold(wtr, |mut state, rbat| async move {
                let rb: &RecordBatch = &rbat;
                state.write(rb).map_err(io::Error::other)?;
                Ok(state)
            })
            .await?;
        wtr.flush().map_err(io::Error::other)?;
        wtr.finish().map_err(io::Error::other)?;
        Ok(())
    }

    pub async fn into_ipc_stream<W>(self, wtr: W) -> Result<(), io::Error>
    where
        W: Write,
    {
        let sch: SchemaRef = self.to_schema();
        let sw = StreamWriter::try_new_buffered(wtr, &sch).map_err(io::Error::other)?;
        self.write_ipc(sw).await
    }

    pub async fn into_ipc_stream_stdout(self) -> Result<(), io::Error> {
        let mut o = io::stdout().lock();
        self.into_ipc_stream(&mut o).await?;
        o.flush()
    }
}

pub async fn ctx2df(ctx: &SessionContext, sql: &str) -> Result<DataFrame, io::Error> {
    ctx.sql(sql).await.map_err(io::Error::other)
}

pub async fn bat2df(rbat: RecordBatch, tabname: &str, sql: &str) -> Result<DataFrame, io::Error> {
    let ctx: SessionContext = SessionContext::new();
    ctx.register_batch(tabname, rbat)
        .map_err(io::Error::other)?;
    ctx2df(&ctx, sql).await
}
