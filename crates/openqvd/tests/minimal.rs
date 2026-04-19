use openqvd::{Qvd, Value};

// Hand-authored minimal QVD: single field "C" with 2 string symbols "A", "B"
// and 2 rows referencing them. This exercises header parse, symbol decode,
// bit extraction, and bias-free lookup end to end. Built from spec alone.
fn minimal_file() -> Vec<u8> {
    let xml = concat!(
        r#"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>"#,
        "\r\n",
        "<QvdTableHeader>\r\n",
        "  <TableName>T</TableName>\r\n",
        "  <Fields>\r\n",
        "    <QvdFieldHeader>\r\n",
        "      <FieldName>C</FieldName>\r\n",
        "      <BitOffset>0</BitOffset>\r\n",
        "      <BitWidth>8</BitWidth>\r\n",
        "      <Bias>0</Bias>\r\n",
        "      <NumberFormat><Type>UNKNOWN</Type></NumberFormat>\r\n",
        "      <NoOfSymbols>2</NoOfSymbols>\r\n",
        "      <Offset>0</Offset>\r\n",
        "      <Length>6</Length>\r\n",
        "      <Tags></Tags>\r\n",
        "    </QvdFieldHeader>\r\n",
        "  </Fields>\r\n",
        "  <Compression></Compression>\r\n",
        "  <RecordByteSize>1</RecordByteSize>\r\n",
        "  <NoOfRecords>2</NoOfRecords>\r\n",
        "  <Offset>6</Offset>\r\n",
        "  <Length>2</Length>\r\n",
        "</QvdTableHeader>\r\n",
    );
    let mut v = xml.as_bytes().to_vec();
    v.push(0x00);
    // Symbol table: 04 'A' 00 04 'B' 00
    v.extend_from_slice(&[0x04, b'A', 0x00, 0x04, b'B', 0x00]);
    // Row block: 00 01
    v.extend_from_slice(&[0x00, 0x01]);
    v
}

#[test]
fn reads_minimal_file() {
    let qvd = Qvd::from_bytes(minimal_file()).expect("decode");
    assert_eq!(qvd.table_name(), "T");
    assert_eq!(qvd.num_rows(), 2);
    assert_eq!(qvd.fields().len(), 1);
    let rows: Vec<_> = qvd.rows().collect();
    assert_eq!(rows.len(), 2);
    match &rows[0][0] {
        Some(Value::Str(s)) => assert_eq!(s, "A"),
        other => panic!("expected Str(\"A\"), got {other:?}"),
    }
    match &rows[1][0] {
        Some(Value::Str(s)) => assert_eq!(s, "B"),
        other => panic!("expected Str(\"B\"), got {other:?}"),
    }
}

#[test]
fn rejects_missing_terminator() {
    let bad = b"<?xml version=\"1.0\"?><QvdTableHeader></QvdTableHeader>".to_vec();
    assert!(Qvd::from_bytes(bad).is_err());
}
