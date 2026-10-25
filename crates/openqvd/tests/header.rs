//! Header parsing: entities, whitespace, required elements, compression,
//! terminator choice and byte-exact rewrite.

use openqvd::{Column, ErrorKind, Qvd, QvdError, ReadOptions, Value, WriteTable};

/// A one-field, one-row file. `table`, `field_name` and `thou` are raw XML
/// text; `field_extra` replaces the numeric field elements when given;
/// `root_extra` is appended inside `<QvdTableHeader>` after the standard
/// root elements.
struct Spec<'a> {
    table: &'a str,
    field_name: &'a str,
    thou: &'a str,
    field_numbers: Option<&'a str>,
    root_numbers: Option<&'a str>,
    compression: &'a str,
    extra_root: &'a str,
    lf: bool,
}

impl Default for Spec<'_> {
    fn default() -> Self {
        Spec {
            table: "T",
            field_name: "c",
            thou: "",
            field_numbers: None,
            root_numbers: None,
            compression: "<Compression></Compression>",
            extra_root: "",
            lf: false,
        }
    }
}

fn build(s: &Spec) -> Vec<u8> {
    let field_numbers = s.field_numbers.unwrap_or(
        "<BitOffset>0</BitOffset><BitWidth>8</BitWidth><Bias>0</Bias>\
         <NoOfSymbols>1</NoOfSymbols><Offset>0</Offset><Length>3</Length>",
    );
    let root_numbers = s.root_numbers.unwrap_or(
        "<RecordByteSize>1</RecordByteSize><NoOfRecords>1</NoOfRecords>\
         <Offset>3</Offset><Length>1</Length>",
    );
    let xml = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n\
         <QvdTableHeader>\r\n<TableName>{table}</TableName>\r\n<Fields>\r\n\
         <QvdFieldHeader><FieldName>{name}</FieldName>{fnums}\
         <NumberFormat><Type>UNKNOWN</Type><Thou>{thou}</Thou></NumberFormat>\
         <Tags></Tags></QvdFieldHeader>\r\n</Fields>\r\n{comp}\r\n{rnums}\r\n{extra}\
         </QvdTableHeader>{eol}",
        table = s.table,
        name = s.field_name,
        fnums = field_numbers,
        thou = s.thou,
        comp = s.compression,
        rnums = root_numbers,
        extra = s.extra_root,
        eol = if s.lf { "\n" } else { "\r\n" },
    );
    let mut v = xml.into_bytes();
    v.push(0);
    v.extend_from_slice(&[0x04, b'Z', 0x00, 0x00]);
    v
}

fn parse(bytes: Vec<u8>) -> Result<Qvd, QvdError> {
    Qvd::parse(bytes, &ReadOptions::default())
}

#[test]
fn baseline_file_parses() {
    let q = parse(build(&Spec::default())).unwrap();
    assert_eq!(q.table_name(), "T");
    assert_eq!(q.fields()[0].name, "c");
}

#[test]
fn predefined_entities_are_resolved_in_names() {
    let q = parse(build(&Spec {
        table: "R&amp;D table",
        field_name: "P&amp;L &lt;net&gt; Customer&apos;s &quot;x&quot;",
        ..Spec::default()
    }))
    .unwrap();
    assert_eq!(q.table_name(), "R&D table");
    assert_eq!(q.fields()[0].name, "P&L <net> Customer's \"x\"");
}

#[test]
fn numeric_character_references_and_cdata() {
    let q = parse(build(&Spec {
        field_name: "A&#x26;B&#67;<![CDATA[ <raw> ]]>",
        ..Spec::default()
    }))
    .unwrap();
    assert_eq!(q.fields()[0].name, "A&BC <raw> ");
}

#[test]
fn unknown_entity_is_an_error() {
    let e = parse(build(&Spec {
        field_name: "a&nbsp;b",
        ..Spec::default()
    }))
    .unwrap_err();
    // quick-xml may reject an undefined entity itself or hand it to us;
    // either way it is a header error, never a silently dropped character.
    assert_eq!(e.kind(), ErrorKind::Header, "{e:?}");
}

#[test]
fn significant_whitespace_is_kept() {
    let q = parse(build(&Spec {
        field_name: " Amount & Tax ".replace('&', "&amp;").leak(),
        thou: " ",
        ..Spec::default()
    }))
    .unwrap();
    assert_eq!(q.fields()[0].name, " Amount & Tax ");
    assert_eq!(q.fields()[0].number_format.thou, " ");
}

#[test]
fn missing_numeric_field_element_is_an_error() {
    // No <BitWidth>.
    let e = parse(build(&Spec {
        field_numbers: Some(
            "<BitOffset>0</BitOffset><Bias>0</Bias>\
             <NoOfSymbols>1</NoOfSymbols><Offset>0</Offset><Length>3</Length>",
        ),
        ..Spec::default()
    }))
    .unwrap_err();
    match e {
        QvdError::MissingElement { element, field } => {
            assert_eq!(element, "BitWidth");
            assert_eq!(field.as_deref(), Some("c"));
        }
        other => panic!("expected MissingElement, got {other:?}"),
    }
}

#[test]
fn self_closing_numeric_element_is_an_error() {
    let e = parse(build(&Spec {
        field_numbers: Some(
            "<BitOffset>0</BitOffset><BitWidth>8</BitWidth><Bias/>\
             <NoOfSymbols>1</NoOfSymbols><Offset>0</Offset><Length>3</Length>",
        ),
        ..Spec::default()
    }))
    .unwrap_err();
    assert!(
        matches!(&e, QvdError::InvalidHeaderValue { element, .. } if element == "Bias"),
        "{e:?}"
    );
}

#[test]
fn duplicate_numeric_element_is_an_error() {
    let e = parse(build(&Spec {
        field_numbers: Some(
            "<BitOffset>0</BitOffset><BitWidth>8</BitWidth><BitWidth>9</BitWidth><Bias>0</Bias>\
             <NoOfSymbols>1</NoOfSymbols><Offset>0</Offset><Length>3</Length>",
        ),
        ..Spec::default()
    }))
    .unwrap_err();
    assert!(
        matches!(&e, QvdError::DuplicateElement { element, .. } if element == "BitWidth"),
        "{e:?}"
    );
}

#[test]
fn missing_root_numbers_are_errors_not_zero_rows() {
    // No NoOfRecords and no root Length: used to open with 0 rows.
    let e = parse(build(&Spec {
        root_numbers: Some("<RecordByteSize>1</RecordByteSize><Offset>3</Offset>"),
        ..Spec::default()
    }))
    .unwrap_err();
    assert!(matches!(e, QvdError::MissingElement { .. }), "{e:?}");
}

#[test]
fn non_empty_compression_is_unsupported() {
    let e = parse(build(&Spec {
        compression: "<Compression>zlib</Compression>",
        ..Spec::default()
    }))
    .unwrap_err();
    assert!(
        matches!(&e, QvdError::UnsupportedCompression { value } if value == "zlib"),
        "{e:?}"
    );
    assert_eq!(e.kind(), ErrorKind::Unsupported);
    // Self-closing and whitespace-only are the empty value.
    parse(build(&Spec {
        compression: "<Compression/>",
        ..Spec::default()
    }))
    .unwrap();
    parse(build(&Spec {
        compression: "<Compression> </Compression>",
        ..Spec::default()
    }))
    .unwrap();
}

#[test]
fn wrong_root_element_is_an_error() {
    let mut b = build(&Spec::default());
    let s = String::from_utf8_lossy(&b).replace("QvdTableHeader", "Other");
    b = s.into_bytes();
    // The terminator no longer matches, so this is "not found".
    assert!(parse(b).is_err());
    let xml = "<Other><TableName>T</TableName></Other>";
    let mut v = xml.as_bytes().to_vec();
    v.extend_from_slice(b"</QvdTableHeader>\r\n\0");
    let e = parse(v).unwrap_err();
    assert!(matches!(e, QvdError::UnexpectedRoot { .. }), "{e:?}");
}

#[test]
fn earliest_terminator_wins_over_a_later_crlf_match_in_the_body() {
    // LF header; the body contains a CRLF-style terminator later on.
    let mut b = build(&Spec {
        lf: true,
        ..Spec::default()
    });
    // Replace the body with bytes containing the CRLF terminator, keeping
    // the declared lengths consistent by using a longer string symbol.
    let header_end = b.iter().position(|&x| x == 0).unwrap() + 1;
    b.truncate(header_end);
    let decoy = b"</QvdTableHeader>\r\n";
    let mut sym = vec![0x04];
    sym.extend_from_slice(decoy);
    sym.push(0);
    let len = sym.len();
    let text = String::from_utf8(b[..header_end - 1].to_vec())
        .unwrap()
        .replace("<Length>3</Length>", &format!("<Length>{len}</Length>"))
        .replace("<Offset>3</Offset>", &format!("<Offset>{len}</Offset>"));
    let mut v = text.into_bytes();
    v.push(0);
    v.extend_from_slice(&sym);
    v.push(0); // the row
    let q = parse(v).unwrap();
    assert_eq!(q.num_rows(), 1);
    let rows: Vec<_> = q.rows().collect::<Result<_, _>>().unwrap();
    assert_eq!(rows[0][0], Some(Value::Str("</QvdTableHeader>\r\n".into())));
}

#[test]
fn to_bytes_returns_the_original_file() {
    let extra = "<QvBuildNo>50699</QvBuildNo><CreatorDoc>abc</CreatorDoc>\
                 <Lineage><LineageInfo><Discriminator>x</Discriminator></LineageInfo></Lineage>";
    let original = build(&Spec {
        table: "R&amp;D",
        thou: " ",
        extra_root: extra,
        ..Spec::default()
    });
    let q = parse(original.clone()).unwrap();
    assert_eq!(q.to_bytes().unwrap(), original);
}

#[test]
fn writer_output_with_special_names_round_trips() {
    let t = WriteTable::new(
        "R&D table",
        vec![Column::new(
            " Amount & Tax <1> Customer's \"q\"",
            vec![Some(Value::Int(1))],
        )],
    )
    .unwrap();
    let q = parse(t.to_bytes().unwrap()).unwrap();
    assert_eq!(q.table_name(), "R&D table");
    assert_eq!(q.fields()[0].name, " Amount & Tax <1> Customer's \"q\"");
}

#[test]
fn number_format_type_is_required_exactly_once() {
    // Type selects the DATE/TIMESTAMP/TIME conversion, so a missing one must
    // not silently read the column as plain numbers.
    let base = String::from_utf8(build(&Spec::default())).unwrap();
    let missing = base.replace("<Type>UNKNOWN</Type>", "");
    let e = parse(missing.into_bytes()).unwrap_err();
    assert!(
        matches!(&e, QvdError::MissingElement { element, .. } if element == "Type"),
        "{e:?}"
    );
    let twice = base.replace(
        "<Type>UNKNOWN</Type>",
        "<Type>DATE</Type><Type>UNKNOWN</Type>",
    );
    let e = parse(twice.into_bytes()).unwrap_err();
    assert!(
        matches!(&e, QvdError::DuplicateElement { element, .. } if element == "Type"),
        "{e:?}"
    );
}
