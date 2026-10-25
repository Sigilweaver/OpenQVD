//! Public API contract: options, leniency, warnings and error kinds.

use openqvd::{
    Column, ErrorKind, Leniency, LenientCheck, Qvd, QvdError, ReadOptions, Value, Warning,
    WarningKind, WriteTable,
};

fn sample() -> Vec<u8> {
    let t = WriteTable::new(
        "t",
        vec![
            Column::new("a", vec![Some(Value::Int(1)), Some(Value::Int(2))]),
            Column::new(
                "b",
                vec![Some(Value::Str("x".into())), Some(Value::Str("y".into()))],
            ),
        ],
    )
    .unwrap();
    t.to_bytes().unwrap()
}

#[test]
fn default_options_are_strict_and_read_everything() {
    let o = ReadOptions::default();
    assert!(o.columns.is_none());
    assert!(o.leniency.is_strict());
    let q = Qvd::parse(sample(), &o).unwrap();
    assert!(q.warnings().is_empty());
    assert_eq!(q.num_rows(), 2);
}

#[test]
fn unknown_projected_column_is_an_error() {
    let o = ReadOptions::new().with_columns(["a", "missing"]);
    let e = Qvd::parse(sample(), &o).unwrap_err();
    match &e {
        QvdError::ColumnNotFound { name } => assert_eq!(name, "missing"),
        other => panic!("expected ColumnNotFound, got {other:?}"),
    }
    assert_eq!(e.kind(), ErrorKind::Schema);
}

#[test]
fn projection_decodes_only_named_columns() {
    let o = ReadOptions::new().with_columns(["a"]);
    let q = Qvd::parse(sample(), &o).unwrap();
    // Both fields stay listed in the header; only `a` has a symbol table.
    assert_eq!(q.fields().len(), 2);
    assert_eq!(q.fields()[1].no_of_symbols, 2);
}

#[test]
fn leniency_is_a_set_of_named_checks() {
    assert!(Leniency::strict().is_strict());
    assert!(Leniency::default().is_strict());
    let l = Leniency::strict().with(LenientCheck::NonNumericTemporal);
    assert!(!l.is_strict());
    assert!(l.allows(LenientCheck::NonNumericTemporal));
    assert_ne!(l, Leniency::lenient());
    assert!(!l.allows(LenientCheck::TimezoneToUtc));
    let all = LenientCheck::ALL
        .iter()
        .fold(Leniency::strict(), |l, c| l.with(*c));
    assert_eq!(all, Leniency::lenient());
    let mut listed: Vec<_> = Leniency::lenient().checks().collect();
    listed.sort();
    let mut expected = LenientCheck::ALL.to_vec();
    expected.sort();
    assert_eq!(listed, expected);
    for c in LenientCheck::ALL {
        assert_eq!(c.as_str().parse::<LenientCheck>().unwrap(), *c);
    }
    assert!("no_such_check".parse::<LenientCheck>().is_err());
}

#[test]
fn warning_builders_and_display() {
    let w = Warning::new(WarningKind::NonNumericTemporal, "3 text symbols")
        .in_column("When")
        .at_row(7);
    assert_eq!(w.column.as_deref(), Some("When"));
    assert_eq!(w.row, Some(7));
    assert_eq!(
        w.to_string(),
        "non_numeric_temporal column \"When\" row 7: 3 text symbols"
    );
    let other = WarningKind::Other("custom_code".into());
    assert_eq!(other.code(), "custom_code");
}

#[test]
fn io_error_keeps_its_source() {
    let e = Qvd::open("/definitely/not/here.qvd", &ReadOptions::default()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Io);
    match &e {
        QvdError::Io(io) => assert_eq!(io.kind(), std::io::ErrorKind::NotFound),
        other => panic!("expected Io, got {other:?}"),
    }
}

#[test]
fn bad_numeric_header_value_names_the_element() {
    let xml = concat!(
        "<?xml version=\"1.0\"?>\r\n<QvdTableHeader>\r\n  <TableName>T</TableName>\r\n",
        "  <Fields>\r\n    <QvdFieldHeader>\r\n      <FieldName>c</FieldName>\r\n",
        "      <BitOffset>0</BitOffset>\r\n      <BitWidth>eight</BitWidth>\r\n",
        "    </QvdFieldHeader>\r\n  </Fields>\r\n</QvdTableHeader>\r\n\0"
    );
    let e = Qvd::parse(xml.as_bytes().to_vec(), &ReadOptions::default()).unwrap_err();
    match &e {
        QvdError::InvalidHeaderValue {
            element,
            value,
            expected,
        } => {
            assert_eq!(element, "BitWidth");
            assert_eq!(value, "eight");
            assert_eq!(*expected, "u32");
        }
        other => panic!("expected InvalidHeaderValue, got {other:?}"),
    }
    assert_eq!(e.kind(), ErrorKind::Header);
}

#[test]
fn error_kinds_cover_each_class() {
    assert_eq!(
        Qvd::parse(b"nope".to_vec(), &ReadOptions::default())
            .unwrap_err()
            .kind(),
        ErrorKind::Header
    );
    let mut bytes = sample();
    bytes.truncate(bytes.len() - 1);
    let e = Qvd::parse(bytes, &ReadOptions::default()).unwrap_err();
    assert_eq!(e.kind(), ErrorKind::Structure);
    assert_eq!(ErrorKind::Structure.as_str(), "structure");
}

#[test]
fn write_table_exposes_read_only_accessors() {
    let t = WriteTable::new("n", vec![Column::new("a", vec![None])]).unwrap();
    assert_eq!(t.name(), "n");
    assert_eq!(t.columns().len(), 1);
    assert_eq!(t.num_rows(), 1);
}
