//! Writer + round-trip tests.

use openqvd::{Column, Dual, Qvd, Value, WriteTable};

fn s(v: &str) -> Option<Value> {
    Some(Value::Str(v.into()))
}

#[test]
fn round_trip_no_nulls() {
    let cols = vec![
        Column::new(
            "id",
            vec![
                Some(Value::Int(1)),
                Some(Value::Int(2)),
                Some(Value::Int(3)),
            ],
        ),
        Column::new("name", vec![s("alpha"), s("beta"), s("alpha")]),
    ];
    let t = WriteTable::new("demo", cols).unwrap();
    let bytes = t.to_bytes().unwrap();
    let q = Qvd::from_bytes(bytes).unwrap();
    assert_eq!(q.num_rows(), 3);
    assert_eq!(q.table_name(), "demo");
    let rows: Vec<_> = q.rows().collect();
    assert_eq!(rows[0][0], Some(Value::Int(1)));
    assert_eq!(rows[0][1], s("alpha"));
    assert_eq!(rows[1][1], s("beta"));
    assert_eq!(rows[2][1], s("alpha"));
    // "alpha" must dedupe to a single symbol.
    assert_eq!(q.symbols(1).unwrap().len(), 2);
}

#[test]
fn round_trip_with_nulls() {
    let cols = vec![Column::new(
        "v",
        vec![s("X"), None, s("Y"), s("X"), None],
    )];
    let t = WriteTable::new("t", cols).unwrap();
    let q = Qvd::from_bytes(t.to_bytes().unwrap()).unwrap();
    let rows: Vec<_> = q.rows().collect();
    assert_eq!(rows[0][0], s("X"));
    assert_eq!(rows[1][0], None);
    assert_eq!(rows[2][0], s("Y"));
    assert_eq!(rows[3][0], s("X"));
    assert_eq!(rows[4][0], None);
    assert_eq!(q.fields()[0].bias, -2);
}

#[test]
fn round_trip_all_types() {
    let cols = vec![
        Column::new("i", vec![Some(Value::Int(-42)), Some(Value::Int(7))]),
        Column::new("f", vec![Some(Value::Float(1.25)), Some(Value::Float(-0.5))]),
        Column::new("s", vec![s("hi"), s("bye")]),
        Column::new(
            "di",
            vec![
                Some(Value::DualInt(Dual { number: 10, text: "ten".into() })),
                Some(Value::DualInt(Dual { number: 20, text: "twenty".into() })),
            ],
        ),
        Column::new(
            "df",
            vec![
                Some(Value::DualFloat(Dual { number: 3.14, text: "pi".into() })),
                Some(Value::DualFloat(Dual { number: 2.72, text: "e".into() })),
            ],
        ),
    ];
    let t = WriteTable::new("big", cols).unwrap();
    let q = Qvd::from_bytes(t.to_bytes().unwrap()).unwrap();
    let rows: Vec<_> = q.rows().collect();
    assert_eq!(rows[0][0], Some(Value::Int(-42)));
    assert_eq!(rows[0][1], Some(Value::Float(1.25)));
    assert_eq!(rows[0][2], s("hi"));
    assert!(matches!(&rows[0][3], Some(Value::DualInt(d)) if d.number == 10 && d.text == "ten"));
    assert!(matches!(&rows[0][4], Some(Value::DualFloat(d)) if d.number == 3.14 && d.text == "pi"));
    assert_eq!(rows[1][2], s("bye"));
}

#[test]
fn empty_table_round_trips() {
    // No rows, but a declared column.
    let t = WriteTable::new("empty", vec![Column::new("x", vec![])]).unwrap();
    let q = Qvd::from_bytes(t.to_bytes().unwrap()).unwrap();
    assert_eq!(q.num_rows(), 0);
    assert_eq!(q.fields().len(), 1);
    assert_eq!(q.fields()[0].bit_width, 0);
}

#[test]
fn single_symbol_column_uses_zero_width() {
    // Three rows, one unique non-null value: should collapse to 0 bits.
    let t = WriteTable::new(
        "k",
        vec![Column::new("c", vec![s("same"), s("same"), s("same")])],
    )
    .unwrap();
    let q = Qvd::from_bytes(t.to_bytes().unwrap()).unwrap();
    assert_eq!(q.fields()[0].bit_width, 0);
    assert_eq!(q.fields()[0].no_of_symbols, 1);
    let rows: Vec<_> = q.rows().collect();
    for r in rows {
        assert_eq!(r[0], s("same"));
    }
}

#[test]
fn wide_cardinality_column() {
    // 500 distinct strings -> bit width 9.
    let mut cells: Vec<_> = (0..500).map(|i| s(&format!("k{i}"))).collect();
    cells.extend((0..500).map(|i| s(&format!("k{i}"))));
    let t = WriteTable::new("w", vec![Column::new("c", cells.clone())]).unwrap();
    let q = Qvd::from_bytes(t.to_bytes().unwrap()).unwrap();
    assert_eq!(q.fields()[0].no_of_symbols, 500);
    assert_eq!(q.fields()[0].bit_width, 9);
    let got: Vec<_> = q.rows().collect();
    for (i, row) in got.iter().enumerate() {
        assert_eq!(row[0], cells[i]);
    }
}

#[test]
fn nul_in_string_is_rejected() {
    let t = WriteTable::new(
        "bad",
        vec![Column::new("x", vec![Some(Value::Str("a\x00b".into()))])],
    )
    .unwrap();
    let err = t.to_bytes().unwrap_err();
    assert!(matches!(err, openqvd::QvdError::Structure(_)));
}

#[test]
fn uneven_columns_rejected() {
    let err = WriteTable::new(
        "uneven",
        vec![
            Column::new("a", vec![s("x")]),
            Column::new("b", vec![s("x"), s("y")]),
        ],
    )
    .unwrap_err();
    assert!(matches!(err, openqvd::QvdError::Structure(_)));
}

#[test]
fn writer_is_deterministic() {
    let cols = vec![
        Column::new(
            "i",
            vec![Some(Value::Int(3)), Some(Value::Int(1)), None, Some(Value::Int(2))],
        ),
        Column::new("s", vec![s("a"), s("b"), s("a"), None]),
    ];
    let t = WriteTable::new("det", cols).unwrap();
    let a = t.to_bytes().unwrap();
    let b = t.to_bytes().unwrap();
    assert_eq!(a, b);
}
