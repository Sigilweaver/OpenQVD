//! Edge-case reader tests derived strictly from SPEC.md observations.

use openqvd::{Qvd, Value};

fn build_qvd(fields_xml: &str, extras: &str, symbols: &[u8], rows: &[u8]) -> Vec<u8> {
    let xml = format!(
        concat!(
            r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>"#,
            "\r\n",
            "<QvdTableHeader>\r\n",
            "  <TableName>T</TableName>\r\n",
            "  <Fields>\r\n{FIELDS}  </Fields>\r\n",
            "  <Compression></Compression>\r\n",
            "{EXTRAS}",
            "</QvdTableHeader>\r\n",
        ),
        FIELDS = fields_xml,
        EXTRAS = extras,
    );
    let mut v = xml.into_bytes();
    v.push(0x00);
    v.extend_from_slice(symbols);
    v.extend_from_slice(rows);
    v
}

/// Bias-based NULL: a 3-bit field with bias=-2 and a stored value of 0
/// must decode as NULL (see NOTES Stage 2 worked example).
#[test]
fn bias_negative_produces_null() {
    let fields = concat!(
        "    <QvdFieldHeader>\r\n",
        "      <FieldName>v</FieldName>\r\n",
        "      <BitOffset>0</BitOffset>\r\n",
        "      <BitWidth>3</BitWidth>\r\n",
        "      <Bias>-2</Bias>\r\n",
        "      <NumberFormat><Type>UNKNOWN</Type></NumberFormat>\r\n",
        "      <NoOfSymbols>2</NoOfSymbols>\r\n",
        "      <Offset>0</Offset>\r\n",
        "      <Length>6</Length>\r\n",
        "      <Tags></Tags>\r\n",
        "    </QvdFieldHeader>\r\n",
    );
    let extras = concat!(
        "  <RecordByteSize>1</RecordByteSize>\r\n",
        "  <NoOfRecords>3</NoOfRecords>\r\n",
        "  <Offset>6</Offset>\r\n",
        "  <Length>3</Length>\r\n",
    );
    // Symbols: 04 'X' 00  04 'Y' 00
    let sym = [0x04, b'X', 0x00, 0x04, b'Y', 0x00];
    // Rows: 0 -> NULL (0 + -2 = -2), 2 -> "X" (2-2=0), 3 -> "Y" (3-2=1)
    let rows = [0x00, 0x02, 0x03];
    let bytes = build_qvd(fields, extras, &sym, &rows);
    let qvd = Qvd::from_bytes(bytes).expect("decode");
    let rs: Vec<_> = qvd.rows().collect();
    assert_eq!(rs[0][0], None);
    assert_eq!(rs[1][0], Some(Value::Str("X".into())));
    assert_eq!(rs[2][0], Some(Value::Str("Y".into())));
}

/// Two packed fields sharing one byte: widths 2 + 6 at bit-offsets 0 and 2.
/// Confirms LSB-first bit extraction across a byte.
#[test]
fn packed_2_plus_6_bits() {
    let fields = concat!(
        "    <QvdFieldHeader>\r\n",
        "      <FieldName>a</FieldName>\r\n",
        "      <BitOffset>0</BitOffset>\r\n",
        "      <BitWidth>2</BitWidth>\r\n",
        "      <Bias>0</Bias>\r\n",
        "      <NumberFormat><Type>UNKNOWN</Type></NumberFormat>\r\n",
        "      <NoOfSymbols>3</NoOfSymbols>\r\n",
        "      <Offset>0</Offset>\r\n",
        "      <Length>9</Length>\r\n",
        "      <Tags></Tags>\r\n",
        "    </QvdFieldHeader>\r\n",
        "    <QvdFieldHeader>\r\n",
        "      <FieldName>b</FieldName>\r\n",
        "      <BitOffset>2</BitOffset>\r\n",
        "      <BitWidth>6</BitWidth>\r\n",
        "      <Bias>0</Bias>\r\n",
        "      <NumberFormat><Type>UNKNOWN</Type></NumberFormat>\r\n",
        "      <NoOfSymbols>3</NoOfSymbols>\r\n",
        "      <Offset>9</Offset>\r\n",
        "      <Length>21</Length>\r\n",
        "      <Tags></Tags>\r\n",
        "    </QvdFieldHeader>\r\n",
    );
    let extras = concat!(
        "  <RecordByteSize>1</RecordByteSize>\r\n",
        "  <NoOfRecords>2</NoOfRecords>\r\n",
        "  <Offset>30</Offset>\r\n",
        "  <Length>2</Length>\r\n",
    );
    // a symbols: "P","Q","R" -> 04 50 00 04 51 00 04 52 00  (9 bytes)
    // b symbols: dual-int (1,"1"),(2,"2"),(3,"3"): 05 01 00 00 00 31 00 (7 b) * 3 = 21
    let mut sym: Vec<u8> = Vec::new();
    sym.extend_from_slice(&[0x04, b'P', 0x00, 0x04, b'Q', 0x00, 0x04, b'R', 0x00]);
    for (n, ch) in [(1i32, b'1'), (2, b'2'), (3, b'3')] {
        sym.push(0x05);
        sym.extend_from_slice(&n.to_le_bytes());
        sym.extend_from_slice(&[ch, 0x00]);
    }
    assert_eq!(sym.len(), 30);
    // Row 0: a=1, b=2  -> byte = (b << 2) | a = (2<<2)|1 = 0x09
    // Row 1: a=2, b=0  -> byte = 0x02
    let rows = [0x09u8, 0x02];
    let qvd = Qvd::from_bytes(build_qvd(fields, extras, &sym, &rows)).expect("decode");
    let got: Vec<_> = qvd.rows().collect();
    assert_eq!(got[0][0], Some(Value::Str("Q".into())));
    assert!(matches!(&got[0][1], Some(Value::DualInt(d)) if d.number == 3 && d.text == "3"));
    assert_eq!(got[1][0], Some(Value::Str("R".into())));
    assert!(matches!(&got[1][1], Some(Value::DualInt(d)) if d.number == 1 && d.text == "1"));
}

/// Zero-width field: BitWidth=0 always resolves to symbol index 0 (spec 3.3).
#[test]
fn zero_width_field_always_picks_symbol_zero() {
    let fields = concat!(
        "    <QvdFieldHeader>\r\n",
        "      <FieldName>c</FieldName>\r\n",
        "      <BitOffset>0</BitOffset>\r\n",
        "      <BitWidth>0</BitWidth>\r\n",
        "      <Bias>0</Bias>\r\n",
        "      <NumberFormat><Type>UNKNOWN</Type></NumberFormat>\r\n",
        "      <NoOfSymbols>1</NoOfSymbols>\r\n",
        "      <Offset>0</Offset>\r\n",
        "      <Length>6</Length>\r\n",
        "      <Tags></Tags>\r\n",
        "    </QvdFieldHeader>\r\n",
    );
    let extras = concat!(
        "  <RecordByteSize>0</RecordByteSize>\r\n",
        "  <NoOfRecords>0</NoOfRecords>\r\n",
        "  <Offset>6</Offset>\r\n",
        "  <Length>0</Length>\r\n",
    );
    let sym = [0x04, b'k', b'o', b'n', b's', 0x00];
    let bytes = build_qvd(fields, extras, &sym, &[]);
    let qvd = Qvd::from_bytes(bytes).expect("decode");
    assert_eq!(qvd.num_rows(), 0);
    assert_eq!(qvd.symbols(0).unwrap().len(), 1);
}

/// A field whose symbol table is all five types at once, to pin decoder
/// behaviour for every variant observed in the corpus.
#[test]
fn all_five_symbol_types() {
    // 0x01 i32, 0x02 f64, 0x04 str, 0x05 dual-i32, 0x06 dual-f64
    // Offsets: 5 + 9 + 4 + 10 + 15 = 43
    let mut sym = Vec::new();
    sym.extend_from_slice(&[0x01]); // type
    sym.extend_from_slice(&(-7i32).to_le_bytes());
    sym.extend_from_slice(&[0x02]);
    sym.extend_from_slice(&1.5f64.to_le_bytes());
    sym.extend_from_slice(&[0x04, b'Z', 0x00]);
    sym.extend_from_slice(&[0x05]);
    sym.extend_from_slice(&42i32.to_le_bytes());
    sym.extend_from_slice(&[b'4', b'2', 0x00]);
    sym.extend_from_slice(&[0x06]);
    sym.extend_from_slice(&3.25f64.to_le_bytes());
    sym.extend_from_slice(&[b'3', b'.', b'2', b'5', 0x00]);

    let length = sym.len() as u32;
    let fields = format!(
        concat!(
            "    <QvdFieldHeader>\r\n",
            "      <FieldName>x</FieldName>\r\n",
            "      <BitOffset>0</BitOffset>\r\n",
            "      <BitWidth>3</BitWidth>\r\n",
            "      <Bias>0</Bias>\r\n",
            "      <NumberFormat><Type>UNKNOWN</Type></NumberFormat>\r\n",
            "      <NoOfSymbols>5</NoOfSymbols>\r\n",
            "      <Offset>0</Offset>\r\n",
            "      <Length>{L}</Length>\r\n",
            "      <Tags></Tags>\r\n",
            "    </QvdFieldHeader>\r\n",
        ),
        L = length,
    );
    let extras = format!(
        concat!(
            "  <RecordByteSize>1</RecordByteSize>\r\n",
            "  <NoOfRecords>5</NoOfRecords>\r\n",
            "  <Offset>{L}</Offset>\r\n",
            "  <Length>5</Length>\r\n",
        ),
        L = length,
    );
    let rows: [u8; 5] = [0, 1, 2, 3, 4];
    let qvd = Qvd::from_bytes(build_qvd(&fields, &extras, &sym, &rows)).expect("decode");
    let got: Vec<_> = qvd.rows().collect();
    assert_eq!(got[0][0], Some(Value::Int(-7)));
    assert_eq!(got[1][0], Some(Value::Float(1.5)));
    assert_eq!(got[2][0], Some(Value::Str("Z".into())));
    assert!(matches!(&got[3][0], Some(Value::DualInt(d)) if d.number == 42 && d.text == "42"));
    assert!(matches!(&got[4][0], Some(Value::DualFloat(d)) if d.number == 3.25 && d.text == "3.25"));
}

/// Unknown symbol type byte must error, not silently misinterpret.
#[test]
fn unknown_type_byte_errors() {
    let fields = concat!(
        "    <QvdFieldHeader>\r\n",
        "      <FieldName>bad</FieldName>\r\n",
        "      <BitOffset>0</BitOffset>\r\n",
        "      <BitWidth>8</BitWidth>\r\n",
        "      <Bias>0</Bias>\r\n",
        "      <NumberFormat><Type>UNKNOWN</Type></NumberFormat>\r\n",
        "      <NoOfSymbols>1</NoOfSymbols>\r\n",
        "      <Offset>0</Offset>\r\n",
        "      <Length>2</Length>\r\n",
        "      <Tags></Tags>\r\n",
        "    </QvdFieldHeader>\r\n",
    );
    let extras = concat!(
        "  <RecordByteSize>1</RecordByteSize>\r\n",
        "  <NoOfRecords>1</NoOfRecords>\r\n",
        "  <Offset>2</Offset>\r\n",
        "  <Length>1</Length>\r\n",
    );
    let sym = [0x7F, 0x00]; // not a known type byte
    let bytes = build_qvd(fields, extras, &sym, &[0x00]);
    let e = Qvd::from_bytes(bytes).unwrap_err();
    match e {
        openqvd::QvdError::UnknownSymbolType { byte, .. } => assert_eq!(byte, 0x7F),
        other => panic!("expected UnknownSymbolType, got {other:?}"),
    }
}

/// Overlapping bit-fields must be rejected (spec 1.4 rule 3).
#[test]
fn overlapping_fields_rejected() {
    let fields = concat!(
        "    <QvdFieldHeader>\r\n",
        "      <FieldName>a</FieldName>\r\n",
        "      <BitOffset>0</BitOffset>\r\n",
        "      <BitWidth>4</BitWidth>\r\n",
        "      <Bias>0</Bias>\r\n",
        "      <NumberFormat><Type>UNKNOWN</Type></NumberFormat>\r\n",
        "      <NoOfSymbols>1</NoOfSymbols>\r\n",
        "      <Offset>0</Offset>\r\n",
        "      <Length>3</Length>\r\n",
        "      <Tags></Tags>\r\n",
        "    </QvdFieldHeader>\r\n",
        "    <QvdFieldHeader>\r\n",
        "      <FieldName>b</FieldName>\r\n",
        "      <BitOffset>2</BitOffset>\r\n",
        "      <BitWidth>4</BitWidth>\r\n",
        "      <Bias>0</Bias>\r\n",
        "      <NumberFormat><Type>UNKNOWN</Type></NumberFormat>\r\n",
        "      <NoOfSymbols>1</NoOfSymbols>\r\n",
        "      <Offset>3</Offset>\r\n",
        "      <Length>3</Length>\r\n",
        "      <Tags></Tags>\r\n",
        "    </QvdFieldHeader>\r\n",
    );
    let extras = concat!(
        "  <RecordByteSize>1</RecordByteSize>\r\n",
        "  <NoOfRecords>1</NoOfRecords>\r\n",
        "  <Offset>6</Offset>\r\n",
        "  <Length>1</Length>\r\n",
    );
    let sym = [0x04, b'q', 0x00, 0x04, b'r', 0x00];
    let bytes = build_qvd(fields, extras, &sym, &[0x00]);
    let e = Qvd::from_bytes(bytes).unwrap_err();
    match e {
        openqvd::QvdError::Structure(msg) => assert!(msg.contains("overlaps"), "msg={msg}"),
        other => panic!("expected Structure error, got {other:?}"),
    }
}

/// Inconsistent root Length (!= n_records * record_byte_size) rejected.
#[test]
fn inconsistent_length_rejected() {
    let fields = concat!(
        "    <QvdFieldHeader>\r\n",
        "      <FieldName>c</FieldName>\r\n",
        "      <BitOffset>0</BitOffset>\r\n",
        "      <BitWidth>8</BitWidth>\r\n",
        "      <Bias>0</Bias>\r\n",
        "      <NumberFormat><Type>UNKNOWN</Type></NumberFormat>\r\n",
        "      <NoOfSymbols>1</NoOfSymbols>\r\n",
        "      <Offset>0</Offset>\r\n",
        "      <Length>3</Length>\r\n",
        "      <Tags></Tags>\r\n",
        "    </QvdFieldHeader>\r\n",
    );
    let extras = concat!(
        "  <RecordByteSize>1</RecordByteSize>\r\n",
        "  <NoOfRecords>2</NoOfRecords>\r\n",
        "  <Offset>3</Offset>\r\n",
        "  <Length>5</Length>\r\n", // bogus
    );
    let sym = [0x04, b'Z', 0x00];
    let bytes = build_qvd(fields, extras, &sym, &[0u8; 2]);
    let e = Qvd::from_bytes(bytes).unwrap_err();
    assert!(matches!(e, openqvd::QvdError::Structure(_)));
}

/// LF-only header terminator variant (observed in 4 corpus files) is
/// accepted.
#[test]
fn lf_terminator_accepted() {
    // Build the minimal file but replace \r\n with \n inside the XML
    // (the terminator is the last line ending).
    let xml = concat!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n",
        "<QvdTableHeader>\n",
        "  <TableName>L</TableName>\n",
        "  <Fields>\n",
        "    <QvdFieldHeader>\n",
        "      <FieldName>c</FieldName>\n",
        "      <BitOffset>0</BitOffset>\n",
        "      <BitWidth>8</BitWidth>\n",
        "      <Bias>0</Bias>\n",
        "      <NumberFormat><Type>UNKNOWN</Type></NumberFormat>\n",
        "      <NoOfSymbols>1</NoOfSymbols>\n",
        "      <Offset>0</Offset>\n",
        "      <Length>3</Length>\n",
        "      <Tags></Tags>\n",
        "    </QvdFieldHeader>\n",
        "  </Fields>\n",
        "  <Compression></Compression>\n",
        "  <RecordByteSize>1</RecordByteSize>\n",
        "  <NoOfRecords>1</NoOfRecords>\n",
        "  <Offset>3</Offset>\n",
        "  <Length>1</Length>\n",
        "</QvdTableHeader>\n",
    );
    let mut v = xml.as_bytes().to_vec();
    v.push(0x00);
    v.extend_from_slice(&[0x04, b'Z', 0x00]);
    v.push(0x00);
    let q = Qvd::from_bytes(v).expect("decode lf");
    assert_eq!(q.num_rows(), 1);
    let r: Vec<_> = q.rows().collect();
    assert_eq!(r[0][0], Some(Value::Str("Z".into())));
}
