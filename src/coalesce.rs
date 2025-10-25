#![forbid(clippy::unwrap_used)]
use std::io;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use arrow::compute::concat_batches;
use arrow_select::coalesce::BatchCoalescer;

pub fn coalesce_batches_from_iter(
    schema: SchemaRef,
    iter: impl Iterator<Item = Result<RecordBatch, io::Error>>,
    max_rows: usize,
) -> Result<RecordBatch, io::Error> {
    let target_batch_size = max_rows;
    let mut coalescer = BatchCoalescer::new(Arc::clone(&schema), target_batch_size);

    for batch_result in iter {
        let batch = batch_result?;
        coalescer.push_batch(batch).map_err(io::Error::other)?;
    }

    coalescer
        .finish_buffered_batch()
        .map_err(io::Error::other)?;

    let mut coalesced_batches = Vec::new();
    while let Some(batch) = coalescer.next_completed_batch() {
        coalesced_batches.push(batch);
    }

    concat_batches(&schema, &coalesced_batches).map_err(io::Error::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use std::io;
    use std::sync::Arc;

    fn create_test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]))
    }

    fn create_test_batch(schema: SchemaRef, a_data: Vec<i32>, b_data: Vec<i32>) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(a_data)),
                Arc::new(Int32Array::from(b_data)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_coalesce_batches_from_iter_empty() {
        let schema = create_test_schema();
        let iter: Vec<Result<RecordBatch, io::Error>> = Vec::new();
        let result = coalesce_batches_from_iter(schema, iter.into_iter(), 10);
        assert!(result.is_ok());
        let batch = result.unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }

    #[test]
    fn test_coalesce_batches_from_iter_single_batch() {
        let schema = create_test_schema();
        let batch = create_test_batch(Arc::clone(&schema), vec![1, 2, 3], vec![4, 5, 6]);
        let iter = vec![Ok(batch.clone())];
        let result = coalesce_batches_from_iter(schema, iter.into_iter(), 10);
        assert!(result.is_ok());
        let coalesced_batch = result.unwrap();
        assert_eq!(coalesced_batch, batch);
    }

    #[test]
    fn test_coalesce_batches_from_iter_multiple_small_batches() {
        let schema = create_test_schema();
        let batch1 = create_test_batch(Arc::clone(&schema), vec![1, 2], vec![10, 11]);
        let batch2 = create_test_batch(Arc::clone(&schema), vec![3, 4, 5], vec![12, 13, 14]);
        let batch3 = create_test_batch(Arc::clone(&schema), vec![6], vec![15]);

        let iter = vec![Ok(batch1), Ok(batch2), Ok(batch3)];
        let result = coalesce_batches_from_iter(schema, iter.into_iter(), 5);
        assert!(result.is_ok());
        let coalesced_batch = result.unwrap();

        assert_eq!(coalesced_batch.num_rows(), 6);
        let expected_batch = create_test_batch(
            create_test_schema(),
            vec![1, 2, 3, 4, 5, 6],
            vec![10, 11, 12, 13, 14, 15],
        );
        assert_eq!(coalesced_batch, expected_batch);
    }

    #[test]
    fn test_coalesce_batches_from_iter_exceed_max_rows() {
        let schema = create_test_schema();
        let batch1 = create_test_batch(Arc::clone(&schema), vec![1, 2, 3, 4], vec![10, 11, 12, 13]);
        let batch2 = create_test_batch(Arc::clone(&schema), vec![5, 6, 7, 8], vec![14, 15, 16, 17]);
        let batch3 = create_test_batch(Arc::clone(&schema), vec![9, 10], vec![18, 19]);

        let iter = vec![Ok(batch1), Ok(batch2), Ok(batch3)];
        let result = coalesce_batches_from_iter(schema, iter.into_iter(), 5);
        assert!(result.is_ok());
        let coalesced_batch = result.unwrap();

        assert_eq!(coalesced_batch.num_rows(), 10);
        let expected_batch = create_test_batch(
            create_test_schema(),
            vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
            vec![10, 11, 12, 13, 14, 15, 16, 17, 18, 19],
        );
        assert_eq!(coalesced_batch, expected_batch);
    }

    #[test]
    fn test_coalesce_batches_from_iter_error_in_stream() {
        let schema = create_test_schema();
        let batch1 = create_test_batch(Arc::clone(&schema), vec![1, 2], vec![10, 11]);
        let error_batch = Err(io::Error::other("test error"));

        let iter = vec![Ok(batch1), error_batch];
        let result = coalesce_batches_from_iter(schema, iter.into_iter(), 10);
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "test error");
    }
}
