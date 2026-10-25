//! Arrow to QVD type mapping: every accepted type is exact, everything else
//! is an error.

#[cfg(any(feature = "arrow-59", feature = "arrow-60"))]
macro_rules! suite {
    ($name:ident, $module:ident, $array:ident, $schema:ident) => {
        mod $name {
            use $array as arrow_array;
            use $schema as arrow_schema;

            use arrow_array::types::*;
            use arrow_array::{
                ArrayRef, BinaryArray, BooleanArray, Date64Array, Decimal128Array,
                DictionaryArray, DurationSecondArray, Float64Array, Int64Array, RecordBatch,
                Time64MicrosecondArray, TimestampMicrosecondArray, TimestampNanosecondArray,
                TimestampSecondArray, UInt32Array, UInt64Array,
            };
            use arrow_schema::{DataType, Field, Schema};
            use openqvd::{
                ColumnType, ColumnValues, Leniency, LenientCheck, Qvd, QvdError, ReadOptions,
                Value, WarningKind, WriteOptions,
            };
            use openqvd_arrow::{$module, BatchOptions};
            use std::sync::Arc;

            fn batch(array: ArrayRef) -> RecordBatch {
                let schema = Schema::new(vec![Field::new("c", array.data_type().clone(), true)]);
                RecordBatch::try_new(Arc::new(schema), vec![array]).unwrap()
            }

            fn write(array: ArrayRef) -> Result<Qvd, QvdError> {
                write_with(array, &WriteOptions::default())
            }

            fn write_with(array: ArrayRef, o: &WriteOptions) -> Result<Qvd, QvdError> {
                let t = $module::record_batch_to_write_table(&batch(array), o)?;
                Qvd::parse(t.to_bytes()?, &ReadOptions::default())
            }

            fn cells(q: &Qvd) -> Vec<Option<Value>> {
                q.rows().map(|r| r.unwrap().remove(0)).collect()
            }

            #[test]
            fn nanosecond_timestamps_are_timestamps_not_text() {
                let ns = 1_700_000_000_000_000_000i64;
                let q = write(Arc::new(TimestampNanosecondArray::from(vec![ns])) as ArrayRef)
                    .unwrap();
                let col = q.column(0).unwrap();
                assert_eq!(col.dtype, ColumnType::TimestampMicros);
                assert_eq!(
                    col.symbols,
                    ColumnValues::TimestampMicros(vec![1_700_000_000_000_000])
                );
            }

            #[test]
            fn whole_second_timestamps_survive_a_round_trip_exactly() {
                let base = 1_700_000_000_000_000i64;
                let v: Vec<i64> = (0..5_000).map(|i| base + i * 1_000_000).collect();
                let q = write(Arc::new(TimestampMicrosecondArray::from(v.clone())) as ArrayRef)
                    .unwrap();
                let b = $module::to_record_batch(&q, &BatchOptions::default()).unwrap();
                let got = b
                    .column(0)
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .unwrap()
                    .values()
                    .to_vec();
                assert_eq!(got, v);
            }

            #[test]
            fn second_timestamps_and_overflow() {
                let q = write(Arc::new(TimestampSecondArray::from(vec![0, 86_400])) as ArrayRef)
                    .unwrap();
                assert_eq!(
                    q.column(0).unwrap().symbols,
                    ColumnValues::TimestampMicros(vec![0, 86_400_000_000])
                );
                let e = write(Arc::new(TimestampSecondArray::from(vec![i64::MAX])) as ArrayRef)
                    .unwrap_err();
                assert!(matches!(e, QvdError::ValueNotRepresentable { .. }), "{e:?}");
            }

            #[test]
            fn time_and_duration_are_times() {
                let q = write(
                    Arc::new(Time64MicrosecondArray::from(vec![45_296_000_000i64])) as ArrayRef,
                )
                .unwrap();
                let col = q.column(0).unwrap();
                assert_eq!(col.dtype, ColumnType::DurationMicros);
                assert_eq!(col.symbols, ColumnValues::DurationMicros(vec![45_296_000_000]));
                match &cells(&q)[0] {
                    Some(Value::DualFloat(d)) => assert_eq!(d.text, "12:34:56"),
                    other => panic!("{other:?}"),
                }
                let q = write(Arc::new(DurationSecondArray::from(vec![3600, 90_000])) as ArrayRef)
                    .unwrap();
                assert_eq!(
                    q.column(0).unwrap().symbols,
                    ColumnValues::DurationMicros(vec![3_600_000_000, 90_000_000_000])
                );
                match &cells(&q)[1] {
                    // 25 hours: the text keeps counting hours.
                    Some(Value::DualFloat(d)) => assert_eq!(d.text, "25:00:00"),
                    other => panic!("{other:?}"),
                }
            }

            #[test]
            fn unsigned_integers_are_not_wrapped() {
                let q = write(
                    Arc::new(UInt32Array::from(vec![3_000_000_000u32, 7])) as ArrayRef,
                )
                .unwrap();
                let c = cells(&q);
                match &c[0] {
                    Some(Value::DualFloat(d)) => {
                        assert_eq!(d.number, 3_000_000_000.0);
                        assert_eq!(d.text, "3000000000");
                    }
                    other => panic!("{other:?}"),
                }
                assert_eq!(c[1], Some(Value::Int(7)));
                assert_eq!(q.column(0).unwrap().dtype, ColumnType::Float64);
            }

            #[test]
            fn large_int64_stays_numeric() {
                let q = write(
                    Arc::new(Int64Array::from(vec![1, 5_000_000_000, -5_000_000_000])) as ArrayRef,
                )
                .unwrap();
                assert_eq!(
                    q.column(0).unwrap().symbols,
                    ColumnValues::Float64(vec![1.0, 5.0e9, -5.0e9])
                );
            }

            #[test]
            fn integers_beyond_2_pow_53_are_errors() {
                for a in [
                    Arc::new(Int64Array::from(vec![(1i64 << 53) + 1])) as ArrayRef,
                    Arc::new(Int64Array::from(vec![i64::MIN])) as ArrayRef,
                    Arc::new(UInt64Array::from(vec![u64::MAX])) as ArrayRef,
                ] {
                    let e = write(a).unwrap_err();
                    assert!(matches!(e, QvdError::ValueNotRepresentable { .. }), "{e:?}");
                }
                // 2^53 itself is exact.
                write(Arc::new(Int64Array::from(vec![1i64 << 53])) as ArrayRef).unwrap();
            }

            #[test]
            fn decimals_keep_their_digits() {
                let a = Decimal128Array::from(vec![Some(12345i128), None, Some(-5)])
                    .with_precision_and_scale(10, 2)
                    .unwrap();
                let q = write(Arc::new(a) as ArrayRef).unwrap();
                let c = cells(&q);
                match &c[0] {
                    Some(Value::DualFloat(d)) => {
                        assert_eq!(d.number, 123.45);
                        assert_eq!(d.text, "123.45");
                    }
                    other => panic!("{other:?}"),
                }
                assert_eq!(c[1], None);
                match &c[2] {
                    Some(Value::DualFloat(d)) => assert_eq!(d.text, "-0.05"),
                    other => panic!("{other:?}"),
                }
            }

            #[test]
            fn decimals_too_wide_for_a_float_are_errors() {
                let a = Decimal128Array::from(vec![123_456_789_012_345_678_901i128])
                    .with_precision_and_scale(38, 2)
                    .unwrap();
                let e = write(Arc::new(a) as ArrayRef).unwrap_err();
                assert!(matches!(e, QvdError::ValueNotRepresentable { .. }), "{e:?}");
            }

            #[test]
            fn time_zone_is_an_error_unless_allowed() {
                let a: ArrayRef = Arc::new(
                    TimestampMicrosecondArray::from(vec![1_000_000i64]).with_timezone("Europe/Paris"),
                );
                let e = write(a.clone()).unwrap_err();
                assert!(
                    matches!(&e, QvdError::TimezoneNotSupported { timezone, .. } if timezone == "Europe/Paris"),
                    "{e:?}"
                );
                let o = WriteOptions::new()
                    .with_leniency(Leniency::strict().with(LenientCheck::TimezoneToUtc));
                let t = $module::record_batch_to_write_table(&batch(a), &o).unwrap();
                assert_eq!(t.warnings().len(), 1);
                assert_eq!(t.warnings()[0].kind, WarningKind::TimezoneToUtc);
                assert_eq!(t.warnings()[0].column.as_deref(), Some("c"));
            }

            #[test]
            fn unmapped_types_are_errors_not_text() {
                let e = write(Arc::new(BinaryArray::from_vec(vec![b"ab"])) as ArrayRef).unwrap_err();
                match e {
                    QvdError::UnsupportedArrowType { column, data_type } => {
                        assert_eq!(column, "c");
                        assert_eq!(data_type, "Binary");
                    }
                    other => panic!("{other:?}"),
                }
            }

            #[test]
            fn date64_must_be_whole_days() {
                write(Arc::new(Date64Array::from(vec![86_400_000i64])) as ArrayRef).unwrap();
                let e = write(Arc::new(Date64Array::from(vec![86_400_001i64])) as ArrayRef)
                    .unwrap_err();
                assert!(matches!(e, QvdError::ValueNotRepresentable { .. }), "{e:?}");
            }

            #[test]
            fn dictionaries_are_unpacked() {
                let d: DictionaryArray<Int8Type> =
                    vec![Some("a"), Some("b"), None, Some("a")].into_iter().collect();
                let q = write(Arc::new(d) as ArrayRef).unwrap();
                let c = cells(&q);
                assert_eq!(c[0], Some(Value::Str("a".into())));
                assert_eq!(c[2], None);
                assert_eq!(c[3], Some(Value::Str("a".into())));
            }

            #[test]
            fn nanosecond_digits_are_an_error_unless_rounding_is_allowed() {
                let a: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
                    1_000_000_000_000_000_001i64,
                    1_000_000_000_000_000_499,
                ]));
                let e = write(a.clone()).unwrap_err();
                assert!(
                    matches!(&e, QvdError::ValueNotRepresentable { detail, .. } if detail.contains("sub_microsecond_precision")),
                    "{e:?}"
                );
                // Whole microseconds are fine.
                write(Arc::new(TimestampNanosecondArray::from(vec![1_000i64, 2_000])) as ArrayRef)
                    .unwrap();
                let o = WriteOptions::new().with_leniency(
                    Leniency::strict().with(LenientCheck::SubMicrosecondPrecision),
                );
                let t = $module::record_batch_to_write_table(&batch(a), &o).unwrap();
                assert_eq!(t.warnings().len(), 1, "one warning per column");
                assert_eq!(t.warnings()[0].kind, WarningKind::SubMicrosecondPrecision);
                // Time64 and Duration in nanoseconds follow the same rule.
                for a in [
                    Arc::new(arrow_array::Time64NanosecondArray::from(vec![1i64])) as ArrayRef,
                    Arc::new(arrow_array::DurationNanosecondArray::from(vec![1i64])) as ArrayRef,
                ] {
                    assert!(matches!(
                        write(a).unwrap_err(),
                        QvdError::ValueNotRepresentable { .. }
                    ));
                }
            }

            #[test]
            fn dates_and_timestamps_the_reader_would_reject_are_not_written() {
                for a in [
                    Arc::new(arrow_array::Date32Array::from(vec![i32::MAX])) as ArrayRef,
                                        Arc::new(TimestampMicrosecondArray::from(vec![i64::MAX])) as ArrayRef,
                    Arc::new(TimestampMicrosecondArray::from(vec![i64::MIN])) as ArrayRef,
                    Arc::new(DurationSecondArray::from(vec![i64::MAX])) as ArrayRef,
                ] {
                    let e = write(a).unwrap_err();
                    assert!(matches!(e, QvdError::ValueNotRepresentable { .. }), "{e:?}");
                }
                // The calendar bounds are accepted and read back strictly.
                for d in [-719_162, 2_932_896] {
                    let q = write(Arc::new(arrow_array::Date32Array::from(vec![d])) as ArrayRef)
                        .unwrap();
                    assert_eq!(q.column(0).unwrap().symbols, ColumnValues::Date32(vec![d]));
                }
                for d in [-719_163, 2_932_897] {
                    assert!(write(Arc::new(arrow_array::Date32Array::from(vec![d])) as ArrayRef)
                        .is_err());
                }
                // Everything the writer accepts re-reads in strict mode.
                let probes = [0i64, 1, -1, 86_400_000_000, 1 << 40, 1 << 55, (1 << 62) + 12_345, -(1 << 61), 253_402_300_799_999_999, -62_135_596_800_000_000];
                for p in probes {
                    let a = Arc::new(TimestampMicrosecondArray::from(vec![p])) as ArrayRef;
                    if let Ok(q) = write(a) {
                        q.column(0).unwrap();
                    }
                }
            }

            #[test]
            fn durations_beyond_a_day_round_trip_as_durations() {
                let q = write(Arc::new(DurationSecondArray::from(vec![3 * 86_400])) as ArrayRef)
                    .unwrap();
                assert_eq!(q.fields()[0].number_format.r#type, "INTERVAL");
                assert_eq!(
                    q.column(0).unwrap().symbols,
                    ColumnValues::DurationMicros(vec![3 * 86_400 * 1_000_000])
                );
            }

            #[test]
            fn duplicate_column_names_are_rejected() {
                let schema = Schema::new(vec![
                    Field::new("a", DataType::Int64, true),
                    Field::new("a", DataType::Int64, true),
                ]);
                let b = RecordBatch::try_new(
                    Arc::new(schema),
                    vec![
                        Arc::new(Int64Array::from(vec![1])) as ArrayRef,
                        Arc::new(Int64Array::from(vec![2])) as ArrayRef,
                    ],
                )
                .unwrap();
                let e = $module::record_batch_to_write_table(&b, &WriteOptions::default())
                    .unwrap_err();
                assert!(matches!(e, QvdError::DuplicateColumn { name } if name == "a"));
            }

            #[test]
            fn booleans_floats_and_nulls() {
                let q = write(
                    Arc::new(BooleanArray::from(vec![Some(true), None, Some(false)])) as ArrayRef,
                )
                .unwrap();
                assert_eq!(
                    cells(&q),
                    vec![Some(Value::Int(1)), None, Some(Value::Int(0))]
                );
                let q = write(Arc::new(Float64Array::from(vec![1.5, f64::NAN])) as ArrayRef)
                    .unwrap();
                assert!(matches!(&cells(&q)[1], Some(Value::Float(f)) if f.is_nan()));
                let q = write(Arc::new(arrow_array::NullArray::new(2)) as ArrayRef).unwrap();
                assert_eq!(cells(&q), vec![None, None]);
                let _ = DataType::Null;
            }
        }
    };
}

#[cfg(feature = "arrow-59")]
suite!(arrow59, v59, arrow_array_59, arrow_schema_59);
#[cfg(feature = "arrow-60")]
suite!(arrow60, v60, arrow_array_60, arrow_schema_60);
