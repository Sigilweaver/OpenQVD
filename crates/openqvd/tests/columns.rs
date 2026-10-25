//! Arrow-free column access and filters.

use openqvd::{
    Column, ColumnFilter, ColumnType, ColumnValues, Dual, Filter, Qvd, QvdError, ReadOptions,
    Value, WriteTable,
};

fn qvd(columns: Vec<Column>, options: &ReadOptions) -> Qvd {
    let bytes = WriteTable::new("t", columns).unwrap().to_bytes().unwrap();
    Qvd::parse(bytes, options).unwrap()
}

fn s(v: &str) -> Option<Value> {
    Some(Value::Str(v.into()))
}

#[test]
fn column_is_dictionary_shaped() {
    let q = qvd(
        vec![Column::new("c", vec![s("x"), None, s("y"), s("x")])],
        &ReadOptions::default(),
    );
    let c = q.column(0).unwrap();
    assert_eq!(c.dtype, ColumnType::Utf8);
    assert_eq!(c.symbols, ColumnValues::Utf8(vec!["x".into(), "y".into()]));
    assert_eq!(c.len(), 4);
    assert_eq!(
        c.iter().collect::<Vec<_>>(),
        vec![Some(0), None, Some(1), Some(0)]
    );
}

#[test]
fn types_follow_symbol_kinds() {
    let q = qvd(
        vec![
            Column::new("i", vec![Some(Value::Int(1)), Some(Value::Int(2))]),
            Column::new("f", vec![Some(Value::Int(1)), Some(Value::Float(2.5))]),
            Column::new(
                "d",
                vec![
                    Some(Value::DualInt(Dual::new(1, "one"))),
                    Some(Value::DualInt(Dual::new(2, "two"))),
                ],
            ),
            Column::new("n", vec![None, None]),
        ],
        &ReadOptions::default(),
    );
    let types: Vec<_> = (0..4).map(|i| q.column(i).unwrap().dtype).collect();
    assert_eq!(
        types,
        vec![
            ColumnType::Int64,
            ColumnType::Float64,
            ColumnType::Int64,
            ColumnType::Null
        ]
    );
}

#[test]
fn undecoded_column_is_an_error_not_nulls() {
    let q = qvd(
        vec![
            Column::new("a", vec![s("x")]),
            Column::new("b", vec![s("y")]),
        ],
        &ReadOptions::new().with_columns(["a"]),
    );
    assert!(q.is_decoded(0));
    assert!(!q.is_decoded(1));
    assert!(q.column(0).is_ok());
    match q.column(1).unwrap_err() {
        QvdError::FieldNotDecoded { field } => assert_eq!(field, "b"),
        other => panic!("expected FieldNotDecoded, got {other:?}"),
    }
}

#[test]
fn filters_select_rows_by_symbol_index() {
    let q = qvd(
        vec![
            Column::new("k", vec![s("a"), s("b"), None, s("a")]),
            Column::new(
                "x",
                vec![
                    Some(Value::Float(f64::NAN)),
                    Some(Value::Float(1.0)),
                    Some(Value::Float(f64::NAN)),
                    Some(Value::Float(2.0)),
                ],
            ),
        ],
        &ReadOptions::default(),
    );
    let sel = |f: Filter| q.select_rows(&[f]).unwrap();
    assert_eq!(sel(Filter::new("k", ColumnFilter::Eq("a".into()))), [0, 3]);
    assert_eq!(
        sel(Filter::new(
            "k",
            ColumnFilter::IsIn(vec!["a".into(), "b".into()])
        )),
        [0, 1, 3]
    );
    assert_eq!(
        sel(Filter::new("k", ColumnFilter::NotIn(vec!["a".into()]))),
        [1, 2]
    );
    assert_eq!(sel(Filter::new("k", ColumnFilter::IsNull)), [2]);
    // NaN symbols must not make rows vanish.
    assert_eq!(sel(Filter::new("x", ColumnFilter::IsNotNull)), [0, 1, 2, 3]);
    assert_eq!(
        q.select_rows(&[
            Filter::new("k", ColumnFilter::Eq("a".into())),
            Filter::new("x", ColumnFilter::IsNotNull)
        ])
        .unwrap(),
        [0, 3]
    );
    assert!(matches!(
        q.select_rows(&[Filter::new("zz", ColumnFilter::IsNull)]),
        Err(QvdError::ColumnNotFound { .. })
    ));
}

#[test]
fn column_select_keeps_chosen_rows() {
    let q = qvd(
        vec![Column::new("c", vec![s("x"), s("y"), s("z")])],
        &ReadOptions::default(),
    );
    let c = q.column(0).unwrap().select(&[0, 2]);
    assert_eq!(c.len(), 2);
    assert_eq!(c.iter().collect::<Vec<_>>(), vec![Some(0), Some(2)]);
}

#[test]
fn rows_and_to_write_table_refuse_undecoded_columns() {
    let q = qvd(
        vec![
            Column::new("a", vec![Some(Value::Int(1)), Some(Value::Int(2))]),
            Column::new("b", vec![s("x"), s("y")]),
        ],
        &ReadOptions::new().with_columns(["a"]),
    );
    let mut rows = q.rows();
    match rows.next() {
        Some(Err(QvdError::FieldNotDecoded { field })) => assert_eq!(field, "b"),
        other => panic!("expected FieldNotDecoded, got {other:?}"),
    }
    assert!(rows.next().is_none());
    assert!(matches!(
        q.to_write_table(),
        Err(QvdError::FieldNotDecoded { .. })
    ));
    // The decoded column is still readable.
    assert_eq!(q.column(0).unwrap().len(), 2);
}
