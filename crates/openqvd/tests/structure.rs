//! Structural invariants checked when a file is opened.

use openqvd::{ErrorKind, Qvd, QvdError, ReadOptions, Value};

struct F {
    name: &'static str,
    bit_offset: u32,
    bit_width: u32,
    bias: i32,
    symbols: u32,
    offset: u32,
    length: u32,
}

fn file(fields: &[F], rbs: u32, n: u32, root_offset: u32, symbols: &[u8], rows: &[u8]) -> Vec<u8> {
    let mut f = String::new();
    for x in fields {
        f.push_str(&format!(
            "<QvdFieldHeader><FieldName>{}</FieldName><BitOffset>{}</BitOffset>\
             <BitWidth>{}</BitWidth><Bias>{}</Bias><NumberFormat><Type>UNKNOWN</Type></NumberFormat>\
             <NoOfSymbols>{}</NoOfSymbols><Offset>{}</Offset><Length>{}</Length><Tags/></QvdFieldHeader>",
            x.name, x.bit_offset, x.bit_width, x.bias, x.symbols, x.offset, x.length
        ));
    }
    let xml = format!(
        "<?xml version=\"1.0\"?>\r\n<QvdTableHeader><TableName>T</TableName><Fields>{f}</Fields>\
         <Compression></Compression><RecordByteSize>{rbs}</RecordByteSize>\
         <NoOfRecords>{n}</NoOfRecords><Offset>{root_offset}</Offset><Length>{}</Length>\
         </QvdTableHeader>\r\n",
        rbs * n
    );
    let mut v = xml.into_bytes();
    v.push(0);
    v.extend_from_slice(symbols);
    v.extend_from_slice(rows);
    v
}

fn open(bytes: Vec<u8>) -> Result<Qvd, QvdError> {
    Qvd::parse(bytes, &ReadOptions::default())
}

fn one(width: u32, bias: i32) -> F {
    F {
        name: "a",
        bit_offset: 0,
        bit_width: width,
        bias,
        symbols: 2,
        offset: 0,
        length: 6,
    }
}

const SYMS: [u8; 6] = [0x04, b'x', 0, 0x04, b'y', 0];

#[test]
fn baseline_opens() {
    let q = open(file(&[one(1, 0)], 1, 2, 6, &SYMS, &[0, 1])).unwrap();
    let rows: Vec<_> = q.rows().collect::<Result<_, _>>().unwrap();
    assert_eq!(rows[1][0], Some(Value::Str("y".into())));
}

#[test]
fn trailing_bytes_after_the_row_block_are_an_error() {
    let e = open(file(&[one(1, 0)], 1, 2, 6, &SYMS, &[0, 1, 9, 9, 9])).unwrap_err();
    assert!(matches!(e, QvdError::TrailingBytes { extra: 3 }), "{e:?}");
    assert_eq!(e.kind(), ErrorKind::Structure);
}

#[test]
fn bits_outside_every_field_must_be_zero() {
    // 1-bit field; rows 0xF0 and 0xF1 carry data in bits no field claims.
    let e = open(file(&[one(1, 0)], 1, 2, 6, &SYMS, &[0xF0, 0xF1])).unwrap_err();
    assert!(
        matches!(e, QvdError::UnusedBitsSet { row: 0, byte: 0 }),
        "{e:?}"
    );
}

#[test]
fn unused_bits_are_checked_in_wide_records_too() {
    // 20-byte records; the field uses 1 bit, a stray bit is set in byte 17.
    let mut row = vec![0u8; 20];
    row[17] = 0x04;
    let e = open(file(&[one(1, 0)], 20, 1, 6, &SYMS, &row)).unwrap_err();
    assert!(
        matches!(e, QvdError::UnusedBitsSet { row: 0, byte: 17 }),
        "{e:?}"
    );
    open(file(&[one(1, 0)], 20, 1, 6, &SYMS, &[0u8; 20])).unwrap();
}

#[test]
fn bit_width_beyond_what_an_index_needs_is_an_error() {
    // 72 bits: the old reader truncated the index to usize and decoded a
    // wrong symbol.
    let mut rows = vec![0u8; 9];
    rows[0] = 1;
    rows[8] = 1; // stored value 2^64 + 1
    let e = open(file(&[one(72, 0)], 9, 1, 6, &SYMS, &rows)).unwrap_err();
    assert!(
        matches!(
            e,
            QvdError::BitWidthOutOfRange {
                bit_width: 72,
                max: 33,
                ..
            }
        ),
        "{e:?}"
    );
    // 33 bits is the widest legal field (u32 symbols plus bias 2).
    open(file(&[one(33, 0)], 5, 1, 6, &SYMS, &[0u8; 5])).unwrap();
}

#[test]
fn unobserved_bias_values_are_unsupported() {
    for bias in [-5, -1, 1, 3] {
        let e = open(file(&[one(3, bias)], 1, 1, 6, &SYMS, &[0])).unwrap_err();
        assert!(
            matches!(e, QvdError::UnsupportedBias { bias: b, .. } if b == bias),
            "{e:?}"
        );
        assert_eq!(e.kind(), ErrorKind::Unsupported);
    }
    open(file(&[one(3, -2)], 1, 1, 6, &SYMS, &[2])).unwrap();
}

#[test]
fn symbol_table_inside_the_row_block_is_an_error() {
    // The field claims bytes [2..6) but the row block starts at 3.
    let mut f = one(1, 0);
    f.offset = 2;
    f.length = 4;
    f.symbols = 1;
    let e = open(file(&[f], 1, 1, 3, &[0, 0, 0], &[0])).unwrap_err();
    assert!(
        matches!(e, QvdError::SymbolRegionInRowBlock { .. }),
        "{e:?}"
    );
}

#[test]
fn overlapping_symbol_tables_are_an_error() {
    let a = F {
        name: "a",
        bit_offset: 0,
        bit_width: 1,
        bias: 0,
        symbols: 2,
        offset: 0,
        length: 6,
    };
    let b = F {
        name: "b",
        bit_offset: 1,
        bit_width: 1,
        bias: 0,
        symbols: 2,
        offset: 3,
        length: 3,
    };
    let e = open(file(&[a, b], 1, 1, 6, &SYMS, &[0])).unwrap_err();
    assert!(matches!(e, QvdError::SymbolRegionOverlap { .. }), "{e:?}");
}

#[test]
fn unclaimed_bytes_between_tables_and_row_block_are_an_error() {
    // Table is [0..6) but the row block starts at 8.
    let e = open(file(
        &[one(1, 0)],
        1,
        1,
        8,
        &[SYMS.as_slice(), &[0, 0]].concat(),
        &[0],
    ))
    .unwrap_err();
    assert!(
        matches!(e, QvdError::SymbolRegionGap { start: 6, end: 8 }),
        "{e:?}"
    );
    // A leading gap.
    let mut f = one(1, 0);
    f.offset = 2;
    let e = open(file(
        &[f],
        1,
        1,
        8,
        &[&[0, 0], SYMS.as_slice()].concat(),
        &[0],
    ))
    .unwrap_err();
    assert!(
        matches!(e, QvdError::SymbolRegionGap { start: 0, end: 2 }),
        "{e:?}"
    );
}

#[test]
fn an_index_past_the_symbol_table_is_rejected_when_the_file_opens() {
    let e = open(file(&[one(2, 0)], 1, 2, 6, &SYMS, &[0, 3])).unwrap_err();
    assert!(
        matches!(
            e,
            QvdError::SymbolIndexOutOfRange {
                row: 1,
                index: 3,
                ..
            }
        ),
        "{e:?}"
    );
}

#[test]
fn undecoded_columns_are_not_validated() {
    // Column projection skips symbol decoding and index checks for fields
    // that were not asked for.
    let bytes = file(&[one(2, 0)], 1, 2, 6, &SYMS, &[0, 3]);
    let q = Qvd::parse(
        bytes,
        &ReadOptions::new().with_columns(Vec::<String>::new()),
    )
    .unwrap();
    assert_eq!(q.num_rows(), 2);
}

#[test]
fn a_huge_record_count_with_zero_width_records_opens_quickly() {
    // Constant columns store no bits; NoOfRecords alone is 2^32 - 1.
    let mut f = one(0, 0);
    f.symbols = 1;
    f.length = 3;
    let t = std::time::Instant::now();
    let q = open(file(&[f], 0, u32::MAX, 3, &[0x04, b'x', 0], &[])).unwrap();
    assert_eq!(q.num_rows(), u32::MAX);
    assert!(
        t.elapsed().as_secs() < 2,
        "validation scaled with NoOfRecords"
    );
}
