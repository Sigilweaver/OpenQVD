//! DATE / TIMESTAMP / TIME conversion.

use openqvd::{
    Column, ColumnType, ColumnValues, Dual, ErrorKind, Leniency, LenientCheck, NumberFormat, Qvd,
    QvdError, ReadOptions, Value, WarningKind, WriteTable,
};

fn temporal(kind: &str, cells: Vec<Option<Value>>) -> Vec<u8> {
    let mut c = Column::new("t", cells);
    c.number_format = NumberFormat::new(kind);
    WriteTable::new("T", vec![c]).unwrap().to_bytes().unwrap()
}

fn strict(bytes: Vec<u8>) -> Result<Qvd, QvdError> {
    Qvd::parse(bytes, &ReadOptions::default())
}

fn lenient(bytes: Vec<u8>) -> Result<Qvd, QvdError> {
    Qvd::parse(
        bytes,
        &ReadOptions::new()
            .with_leniency(Leniency::strict().with(LenientCheck::NonNumericTemporal)),
    )
}

fn float(n: f64) -> Option<Value> {
    Some(Value::DualFloat(Dual::new(n, "x")))
}

#[test]
fn whole_second_timestamps_are_exact() {
    // The nearest f64 to the exact instant, as an application storing the
    // serial with one rounding would write it. (Two roundings can put the
    // stored value half a microsecond or more off the instant.)
    let base_us: i64 = 1_700_000_000_000_000; // 2023-11-14 22:13:20
    let cells: Vec<_> = (0..10_000i64)
        .map(|i| {
            let us = base_us + i * 1_000_000;
            float((us + 25_569 * 86_400_000_000) as f64 / 86_400_000_000.0)
        })
        .collect();
    let q = strict(temporal("TIMESTAMP", cells)).unwrap();
    let col = q.column(0).unwrap();
    assert_eq!(col.dtype, ColumnType::TimestampMicros);
    let ColumnValues::TimestampMicros(v) = &col.symbols else {
        panic!("wrong values")
    };
    let wrong = col
        .iter()
        .enumerate()
        .filter(|(i, s)| v[s.unwrap()] != base_us + *i as i64 * 1_000_000)
        .count();
    assert_eq!(wrong, 0, "truncation used to be 1 us early for ~30%");
}

#[test]
fn time_of_day_is_rounded_not_truncated() {
    let q = strict(temporal("TIME", vec![float(0.5), float(1.0 / 3.0)])).unwrap();
    let ColumnValues::DurationMicros(v) = &q.column(0).unwrap().symbols else {
        panic!()
    };
    assert_eq!(v[0], 43_200_000_000);
    assert_eq!(v[1], 28_800_000_000); // 1/3 day = 8 h, to the microsecond
}

#[test]
fn text_in_a_date_column_is_an_error_by_default() {
    let bytes = temporal(
        "DATE",
        vec![float(45_000.0), Some(Value::Str("N/A".into())), None],
    );
    let e = strict(bytes).unwrap_err();
    match &e {
        QvdError::NonNumericTemporal {
            field,
            hint,
            symbol,
            ..
        } => {
            assert_eq!(field, "t");
            assert_eq!(hint, "DATE");
            assert!(symbol.contains("N/A"), "{symbol}");
        }
        other => panic!("expected NonNumericTemporal, got {other:?}"),
    }
    assert_eq!(e.kind(), ErrorKind::Conversion);
}

#[test]
fn lenient_mode_returns_text_and_a_warning() {
    let bytes = temporal(
        "DATE",
        vec![float(45_000.0), Some(Value::Str("N/A".into())), None],
    );
    let q = lenient(bytes).unwrap();
    let w = q.warnings();
    assert_eq!(w.len(), 1);
    assert_eq!(w[0].kind, WarningKind::NonNumericTemporal);
    assert_eq!(w[0].column.as_deref(), Some("t"));
    assert!(w[0].detail.contains("N/A"), "{}", w[0].detail);
    let col = q.column(0).unwrap();
    assert_eq!(col.dtype, ColumnType::Utf8);
    let ColumnValues::Utf8(v) = &col.symbols else {
        panic!()
    };
    assert_eq!(v, &["x".to_string(), "N/A".to_string()]);
}

#[test]
fn nan_and_infinity_are_not_dates() {
    for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let e = strict(temporal("TIMESTAMP", vec![float(bad)])).unwrap_err();
        assert!(matches!(e, QvdError::NonNumericTemporal { .. }), "{e:?}");
        let q = lenient(temporal("TIMESTAMP", vec![float(bad)])).unwrap();
        assert_eq!(q.warnings().len(), 1);
    }
}

#[test]
fn integer_dates_stay_dates() {
    let q = strict(temporal(
        "DATE",
        vec![Some(Value::Int(25_569)), Some(Value::Int(45_000))],
    ))
    .unwrap();
    let col = q.column(0).unwrap();
    assert_eq!(col.dtype, ColumnType::Date32);
    assert_eq!(col.symbols, ColumnValues::Date32(vec![0, 45_000 - 25_569]));
}

#[test]
fn a_date_with_a_time_part_becomes_a_timestamp() {
    // Qlik's Date(Now()) keeps the fraction; truncating would drop 18:00.
    let q = strict(temporal("DATE", vec![float(25_569.75), float(25_570.0)])).unwrap();
    let col = q.column(0).unwrap();
    assert_eq!(col.dtype, ColumnType::TimestampMicros);
    assert_eq!(
        col.symbols,
        ColumnValues::TimestampMicros(vec![64_800_000_000, 86_400_000_000])
    );
    assert!(q.warnings().is_empty());
}

#[test]
fn negative_serials_keep_their_time_of_day() {
    // -0.5 is 1899-12-29 12:00: truncating toward zero used to give 12-30.
    let q = strict(temporal("DATE", vec![float(-0.5)])).unwrap();
    let col = q.column(0).unwrap();
    assert_eq!(col.dtype, ColumnType::TimestampMicros);
    assert_eq!(
        col.symbols,
        ColumnValues::TimestampMicros(vec![-2_209_204_800_000_000])
    );
}

#[test]
fn out_of_range_serials_are_errors_not_wraps() {
    for (kind, v) in [
        ("DATE", 3.0e9),
        ("DATE", -3.0e9),
        ("TIMESTAMP", 1.0e300),
        ("TIME", 1.0e15),
    ] {
        let e = strict(temporal(kind, vec![float(v)])).unwrap_err();
        assert!(
            matches!(e, QvdError::TemporalOutOfRange { .. }),
            "{kind} {v}: {e:?}"
        );
    }
}

#[test]
fn non_temporal_columns_are_unaffected() {
    let mut c = Column::new("t", vec![Some(Value::Str("N/A".into()))]);
    c.number_format = NumberFormat::new("UNKNOWN");
    let q = strict(WriteTable::new("T", vec![c]).unwrap().to_bytes().unwrap()).unwrap();
    assert_eq!(q.column(0).unwrap().dtype, ColumnType::Utf8);
}
