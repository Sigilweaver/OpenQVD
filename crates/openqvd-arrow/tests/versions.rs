//! The same adapter tests, run against every enabled Arrow major.

#[cfg(any(feature = "arrow-59", feature = "arrow-60"))]
macro_rules! suite {
    ($name:ident, $module:ident, $array:ident, $schema:ident) => {
        mod $name {
            use $array as arrow_array;
            use $schema as arrow_schema;

            use arrow_array::{
                cast::AsArray, types::*, Array, ArrayRef, Float64Array, Int64Array,
                LargeStringArray, RecordBatch, StringArray,
            };
            use arrow_schema::{DataType, Field, Schema, TimeUnit};
            use openqvd::{
                Column, ColumnFilter, Filter, Qvd, QvdError, ReadOptions, Value, WriteOptions,
                WriteTable,
            };
            use openqvd_arrow::{$module, BatchOptions};
            use std::sync::Arc;

            fn sample() -> Qvd {
                let t = WriteTable::new(
                    "t",
                    vec![
                        Column::new("i", vec![Some(Value::Int(1)), None, Some(Value::Int(3))]),
                        Column::new(
                            "s",
                            vec![
                                Some(Value::Str("a".into())),
                                Some(Value::Str("b".into())),
                                Some(Value::Str("c".into())),
                            ],
                        ),
                        Column::new(
                            "f",
                            vec![
                                Some(Value::Float(1.5)),
                                Some(Value::Float(2.5)),
                                Some(Value::Float(3.5)),
                            ],
                        ),
                    ],
                )
                .unwrap();
                Qvd::parse(t.to_bytes().unwrap(), &ReadOptions::default()).unwrap()
            }

            #[test]
            fn reads_types_and_nulls() {
                let b = $module::to_record_batch(&sample(), &BatchOptions::default()).unwrap();
                assert_eq!(b.num_rows(), 3);
                assert_eq!(b.schema().field(0).data_type(), &DataType::Int64);
                assert_eq!(b.schema().field(1).data_type(), &DataType::LargeUtf8);
                assert_eq!(b.schema().field(2).data_type(), &DataType::Float64);
                let i = b.column(0).as_primitive::<Int64Type>();
                assert!(i.is_null(1));
                assert_eq!(i.value(2), 3);
                let s = b.column(1).as_string::<i64>();
                assert_eq!(s.value(1), "b");
            }

            #[test]
            fn projection_and_filters() {
                let opts = BatchOptions::new()
                    .with_columns(["s"])
                    .with_filters(vec![Filter::new("i", ColumnFilter::IsNotNull)]);
                let b = $module::to_record_batch(&sample(), &opts).unwrap();
                assert_eq!(b.num_columns(), 1);
                assert_eq!(b.num_rows(), 2);
                let s = b.column(0).as_string::<i64>();
                assert_eq!((s.value(0), s.value(1)), ("a", "c"));
            }

            #[test]
            fn unknown_column_is_an_error() {
                let e = $module::to_record_batch(
                    &sample(),
                    &BatchOptions::new().with_columns(["nope"]),
                )
                .unwrap_err();
                assert!(matches!(e, QvdError::ColumnNotFound { .. }));
            }

            #[test]
            fn all_null_column_survives_filtering() {
                let t = WriteTable::new(
                    "t",
                    vec![
                        Column::new("n", vec![None, None, None]),
                        Column::new(
                            "k",
                            vec![
                                Some(Value::Int(1)),
                                Some(Value::Int(2)),
                                Some(Value::Int(1)),
                            ],
                        ),
                    ],
                )
                .unwrap();
                let q = Qvd::parse(t.to_bytes().unwrap(), &ReadOptions::default()).unwrap();
                let opts = BatchOptions::new()
                    .with_filters(vec![Filter::new("k", ColumnFilter::Eq("1".into()))]);
                let b = $module::to_record_batch(&q, &opts).unwrap();
                assert_eq!(b.num_rows(), 2);
                assert_eq!(b.column(0).len(), 2);
            }

            #[test]
            fn write_then_read_round_trip() {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("x", DataType::Int64, true),
                    Field::new("y", DataType::Utf8, true),
                    Field::new("z", DataType::Float64, true),
                    Field::new("d", DataType::Date32, true),
                    Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), true),
                ]));
                let cols: Vec<ArrayRef> = vec![
                    Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                    Arc::new(Float64Array::from(vec![0.5, 1.5, 2.5])),
                    Arc::new(arrow_array::Date32Array::from(vec![0, 1, 19000])),
                    Arc::new(arrow_array::TimestampMicrosecondArray::from(vec![
                        0,
                        1_000_000,
                        86_400_000_000,
                    ])),
                ];
                let batch = RecordBatch::try_new(schema, cols).unwrap();
                let opts = WriteOptions::new().with_table_name("rt");
                let wt = $module::record_batch_to_write_table(&batch, &opts).unwrap();
                assert_eq!(wt.name(), "rt");
                let q = Qvd::parse(wt.to_bytes().unwrap(), &ReadOptions::default()).unwrap();
                let back = $module::to_record_batch(&q, &BatchOptions::default()).unwrap();
                assert_eq!(back.num_rows(), 3);
                assert_eq!(
                    back.column(1)
                        .as_any()
                        .downcast_ref::<LargeStringArray>()
                        .unwrap()
                        .value(2),
                    "c"
                );
                assert_eq!(
                    back.column(3).as_primitive::<Date32Type>().values(),
                    &[0, 1, 19000]
                );
            }

            #[test]
            fn every_batch_is_converted() {
                let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
                let mk = |v: Vec<i64>| {
                    RecordBatch::try_new(
                        schema.clone(),
                        vec![Arc::new(Int64Array::from(v)) as ArrayRef],
                    )
                    .unwrap()
                };
                let batches = vec![mk(vec![1, 2]), mk(vec![3, 4, 5])];
                let wt = $module::record_batches_to_write_table(
                    schema.clone(),
                    &batches,
                    &WriteOptions::default(),
                )
                .unwrap();
                assert_eq!(wt.num_rows(), 5);
            }

            #[test]
            fn batches_with_different_schemas_are_rejected() {
                let a = Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, true)]));
                let b = Arc::new(Schema::new(vec![Field::new("x", DataType::Float64, true)]));
                let batches = vec![RecordBatch::try_new(
                    b,
                    vec![Arc::new(Float64Array::from(vec![1.0])) as ArrayRef],
                )
                .unwrap()];
                let e =
                    $module::record_batches_to_write_table(a, &batches, &WriteOptions::default())
                        .unwrap_err();
                assert!(matches!(e, QvdError::Arrow { .. }));
            }
        }
    };
}

#[cfg(feature = "arrow-59")]
suite!(arrow59, v59, arrow_array_59, arrow_schema_59);
#[cfg(feature = "arrow-60")]
suite!(arrow60, v60, arrow_array_60, arrow_schema_60);
