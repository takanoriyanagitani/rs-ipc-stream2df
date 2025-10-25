#![forbid(clippy::unwrap_used)]
use std::io::{self, BufReader, Read};

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::SchemaRef;

use crate::coalesce::coalesce_batches_from_iter;

pub struct IpcStreamReader<R>(pub StreamReader<BufReader<R>>);

impl<R> IpcStreamReader<R>
where
    R: Read,
{
    pub fn schema_ref(&self) -> SchemaRef {
        self.0.schema()
    }

    pub fn into_batch_iter(self) -> impl Iterator<Item = Result<RecordBatch, io::Error>> {
        self.0.map(|r| r.map_err(io::Error::other))
    }

    pub fn into_batch(self, max_rows: usize) -> Result<RecordBatch, io::Error> {
        let schema = self.0.schema();
        coalesce_batches_from_iter(schema, self.into_batch_iter(), max_rows)
    }
}

impl<R> IpcStreamReader<R>
where
    R: Read,
{
    pub fn from_rdr(rdr: R, projection: Option<Vec<usize>>) -> Result<Self, io::Error> {
        let srdr = StreamReader::try_new_buffered(rdr, projection).map_err(io::Error::other)?;
        Ok(Self(srdr))
    }
}
