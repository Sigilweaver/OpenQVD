//! `openqvd` command-line tool.
//!
//! Subcommands:
//!   stat <file>           Print header summary (fields, widths, rows).
//!   head <file> [--rows N]  Print the first N rows (default 10).
//!   csv  <file>           Print every row as CSV (tab-separated values
//!                          for simplicity; NULLs become empty strings).
//!   json <file>           Print every row as one JSON object per line.
//!   rewrite <in> <out>    Read <in>, re-serialise to <out> via the
//!                          writer. Useful for smoke-testing the writer.
//!
//! All output goes to stdout unless specified. Errors exit non-zero.

use std::io::{self, Write};
use std::process::ExitCode;

use openqvd::{Qvd, Value};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(cmd) = args.next() else {
        eprintln!("{}", USAGE);
        return ExitCode::from(2);
    };
    let rest: Vec<String> = args.collect();
    let res = match cmd.as_str() {
        "stat" => cmd_stat(&rest),
        "head" => cmd_head(&rest),
        "csv" => cmd_csv(&rest),
        "json" => cmd_json(&rest),
        "rewrite" => cmd_rewrite(&rest),
        "-h" | "--help" | "help" => {
            println!("{}", USAGE);
            return ExitCode::SUCCESS;
        }
        _ => {
            eprintln!("unknown command: {cmd}\n{}", USAGE);
            return ExitCode::from(2);
        }
    };
    match res {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

const USAGE: &str = "\
openqvd - Qlik QVD reader/writer (AGPL-3.0-or-later)

Usage:
  openqvd stat <file>
  openqvd head <file> [--rows N]
  openqvd csv  <file>
  openqvd json <file>
  openqvd rewrite <in> <out>
";

fn cmd_stat(args: &[String]) -> Result<(), String> {
    let path = args.first().ok_or("stat: missing file")?;
    let q = Qvd::from_path(path).map_err(|e| e.to_string())?;
    let mut out = io::BufWriter::new(io::stdout().lock());
    writeln!(out, "table: {}", q.table_name()).unwrap();
    writeln!(out, "rows:  {}", q.num_rows()).unwrap();
    writeln!(
        out,
        "record_byte_size: {}  row_block: offset={} length={}",
        q.header().record_byte_size,
        q.header().row_block_offset,
        q.header().row_block_length,
    )
    .unwrap();
    writeln!(out, "fields ({}):", q.fields().len()).unwrap();
    for (i, f) in q.fields().iter().enumerate() {
        writeln!(
            out,
            "  [{i:>2}] {name:<32}  bits@{off}+{w:<2}  bias={bias:<3}  \
             n_sym={ns:<6}  type={ty}  tags={tg}",
            i = i,
            name = f.name,
            off = f.bit_offset,
            w = f.bit_width,
            bias = f.bias,
            ns = f.no_of_symbols,
            ty = f.number_format.r#type,
            tg = if f.tags.is_empty() {
                String::new()
            } else {
                f.tags.join(" ")
            },
        )
        .unwrap();
    }
    Ok(())
}

fn cmd_head(args: &[String]) -> Result<(), String> {
    let path = args.first().ok_or("head: missing file")?;
    let mut n_rows: usize = 10;
    let mut i = 1;
    while i < args.len() {
        if args[i] == "--rows" {
            n_rows = args
                .get(i + 1)
                .ok_or("head: --rows needs a value")?
                .parse()
                .map_err(|_| "head: invalid --rows value")?;
            i += 2;
        } else {
            return Err(format!("head: unknown argument {:?}", args[i]));
        }
    }
    let q = Qvd::from_path(path).map_err(|e| e.to_string())?;
    let mut out = io::BufWriter::new(io::stdout().lock());
    print_header_row(&mut out, &q);
    for row in q.rows().take(n_rows) {
        print_value_row(&mut out, &row);
    }
    Ok(())
}

fn cmd_csv(args: &[String]) -> Result<(), String> {
    let path = args.first().ok_or("csv: missing file")?;
    let q = Qvd::from_path(path).map_err(|e| e.to_string())?;
    let mut out = io::BufWriter::new(io::stdout().lock());
    print_header_row(&mut out, &q);
    for row in q.rows() {
        print_value_row(&mut out, &row);
    }
    Ok(())
}

fn cmd_json(args: &[String]) -> Result<(), String> {
    let path = args.first().ok_or("json: missing file")?;
    let q = Qvd::from_path(path).map_err(|e| e.to_string())?;
    let mut out = io::BufWriter::new(io::stdout().lock());
    let names: Vec<&str> = q.fields().iter().map(|f| f.name.as_str()).collect();
    for row in q.rows() {
        let mut first = true;
        write!(out, "{{").unwrap();
        for (name, cell) in names.iter().zip(&row) {
            if !first {
                write!(out, ",").unwrap();
            }
            first = false;
            write!(out, "\"").unwrap();
            json_write_str(&mut out, name);
            write!(out, "\":").unwrap();
            write_json_value(&mut out, cell);
        }
        writeln!(out, "}}").unwrap();
    }
    Ok(())
}

fn cmd_rewrite(args: &[String]) -> Result<(), String> {
    let src = args.first().ok_or("rewrite: missing <in>")?;
    let dst = args.get(1).ok_or("rewrite: missing <out>")?;
    let q = Qvd::from_path(src).map_err(|e| e.to_string())?;
    q.write_to_path(dst).map_err(|e| e.to_string())?;
    eprintln!("wrote {}", dst);
    Ok(())
}

fn print_header_row<W: Write>(out: &mut W, q: &Qvd) {
    let mut first = true;
    for f in q.fields() {
        if !first {
            write!(out, "\t").unwrap();
        }
        first = false;
        write!(out, "{}", f.name).unwrap();
    }
    writeln!(out).unwrap();
}

fn print_value_row<W: Write>(out: &mut W, row: &[Option<Value>]) {
    let mut first = true;
    for cell in row {
        if !first {
            write!(out, "\t").unwrap();
        }
        first = false;
        match cell {
            None => {}
            Some(Value::Int(i)) => write!(out, "{i}").unwrap(),
            Some(Value::Float(f)) => write!(out, "{f}").unwrap(),
            Some(Value::Str(s)) => write!(out, "{}", s.replace(['\t', '\n'], " ")).unwrap(),
            Some(Value::DualInt(d)) => write!(out, "{}", d.text.replace(['\t', '\n'], " ")).unwrap(),
            Some(Value::DualFloat(d)) => {
                write!(out, "{}", d.text.replace(['\t', '\n'], " ")).unwrap()
            }
        }
    }
    writeln!(out).unwrap();
}

fn write_json_value<W: Write>(out: &mut W, cell: &Option<Value>) {
    match cell {
        None => write!(out, "null").unwrap(),
        Some(Value::Int(i)) => write!(out, "{i}").unwrap(),
        Some(Value::Float(f)) => {
            if f.is_finite() {
                write!(out, "{f}").unwrap()
            } else {
                write!(out, "null").unwrap()
            }
        }
        Some(Value::Str(s)) => {
            write!(out, "\"").unwrap();
            json_write_str(out, s);
            write!(out, "\"").unwrap();
        }
        Some(Value::DualInt(d)) => write!(out, "{}", d.number).unwrap(),
        Some(Value::DualFloat(d)) => {
            if d.number.is_finite() {
                write!(out, "{}", d.number).unwrap()
            } else {
                write!(out, "\"").unwrap();
                json_write_str(out, &d.text);
                write!(out, "\"").unwrap();
            }
        }
    }
}

fn json_write_str<W: Write>(out: &mut W, s: &str) {
    for c in s.chars() {
        match c {
            '"' => write!(out, "\\\"").unwrap(),
            '\\' => write!(out, "\\\\").unwrap(),
            '\n' => write!(out, "\\n").unwrap(),
            '\r' => write!(out, "\\r").unwrap(),
            '\t' => write!(out, "\\t").unwrap(),
            c if (c as u32) < 0x20 => write!(out, "\\u{:04x}", c as u32).unwrap(),
            c => write!(out, "{c}").unwrap(),
        }
    }
}
