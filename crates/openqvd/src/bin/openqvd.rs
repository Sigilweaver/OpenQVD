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

use openqvd::{Leniency, LenientCheck, Qvd, ReadOptions, Value};

fn main() -> ExitCode {
    let mut args = std::env::args().skip(1);
    let Some(cmd) = args.next() else {
        eprintln!("{}", USAGE);
        return ExitCode::from(2);
    };
    let rest: Vec<String> = args.collect();
    let (leniency, rest) = match extract_leniency(&rest) {
        Ok(x) => x,
        Err(e) => {
            eprintln!("error: {e}\n{}", USAGE);
            return ExitCode::from(2);
        }
    };
    let res = match cmd.as_str() {
        "stat" => cmd_stat(&rest, &leniency),
        "head" => cmd_head(&rest, &leniency),
        "csv" => cmd_csv(&rest, &leniency),
        "json" => cmd_json(&rest, &leniency),
        "rewrite" => cmd_rewrite(&rest, &leniency),
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
openqvd - Qlik QVD reader/writer (Apache-2.0)

Usage:
  openqvd stat <file>
  openqvd head <file> [--rows N]
  openqvd csv  <file>
  openqvd json <file>
  openqvd rewrite <in> <out>

Reading is strict. To accept a named deviation and get a warning on stderr:
  --allow <check>   (repeatable) e.g. --allow non_numeric_temporal
  --lenient         allow every check this version knows
Checks: non_numeric_temporal, timezone_to_utc, sub_microsecond_precision
";

/// Split `--allow <check>` and `--lenient` out of the arguments.
fn extract_leniency(args: &[String]) -> Result<(Leniency, Vec<String>), String> {
    let mut leniency = Leniency::strict();
    let mut rest = Vec::with_capacity(args.len());
    let mut it = args.iter();
    while let Some(a) = it.next() {
        match a.as_str() {
            "--lenient" => leniency = Leniency::lenient(),
            "--allow" => {
                let name = it.next().ok_or("--allow needs a check name")?;
                let check: LenientCheck = name.parse().map_err(|e| format!("{e}"))?;
                leniency = leniency.with(check);
            }
            _ => rest.push(a.clone()),
        }
    }
    Ok((leniency, rest))
}

/// Open a file with the given leniency, rendering the error with its source
/// chain and printing what a lenient read tolerated to stderr.
fn open_qvd(path: &str, leniency: &Leniency) -> Result<Qvd, String> {
    let options = ReadOptions::default().with_leniency(leniency.clone());
    let q = Qvd::open(path, &options).map_err(|e| chain(&e))?;
    for w in q.warnings() {
        eprintln!("warning: {w}");
    }
    Ok(q)
}

/// Flush buffered output and report a failure: an unflushed error would
/// otherwise be dropped and the command would exit 0 with truncated output.
/// A closed pipe (`openqvd csv f | head`) is the reader's choice, not an
/// error.
fn finish<W: Write>(mut out: W) -> Result<(), String> {
    match out.flush() {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => Ok(()),
        Err(e) => Err(format!("writing output: {e}")),
    }
}

/// Render an error followed by each of its sources, `: `-separated.
fn chain(e: &dyn std::error::Error) -> String {
    let mut out = e.to_string();
    let mut src = e.source();
    while let Some(s) = src {
        let text = s.to_string();
        if !out.contains(&text) {
            out.push_str(": ");
            out.push_str(&text);
        }
        src = s.source();
    }
    out
}

fn cmd_stat(args: &[String], leniency: &Leniency) -> Result<(), String> {
    let path = args.first().ok_or("stat: missing file")?;
    let q = open_qvd(path, leniency)?;
    let mut out = io::BufWriter::new(io::stdout().lock());
    writeln!(out, "table: {}", q.table_name()).map_err(|e| e.to_string())?;
    writeln!(out, "rows:  {}", q.num_rows()).map_err(|e| e.to_string())?;
    writeln!(
        out,
        "record_byte_size: {}  row_block: offset={} length={}",
        q.header().record_byte_size,
        q.header().row_block_offset,
        q.header().row_block_length,
    )
    .map_err(|e| e.to_string())?;
    writeln!(out, "fields ({}):", q.fields().len()).map_err(|e| e.to_string())?;
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
        .map_err(|e| e.to_string())?;
    }
    finish(out)
}

fn cmd_head(args: &[String], leniency: &Leniency) -> Result<(), String> {
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
    let q = open_qvd(path, leniency)?;
    let mut out = io::BufWriter::new(io::stdout().lock());
    print_header_row(&mut out, &q)?;
    for row in q.rows().take(n_rows) {
        let row = row.map_err(|e| chain(&e))?;
        print_value_row(&mut out, &row)?;
    }
    finish(out)
}

fn cmd_csv(args: &[String], leniency: &Leniency) -> Result<(), String> {
    let path = args.first().ok_or("csv: missing file")?;
    let q = open_qvd(path, leniency)?;
    let mut out = io::BufWriter::new(io::stdout().lock());
    print_header_row(&mut out, &q)?;
    for row in q.rows() {
        let row = row.map_err(|e| chain(&e))?;
        print_value_row(&mut out, &row)?;
    }
    finish(out)
}

fn cmd_json(args: &[String], leniency: &Leniency) -> Result<(), String> {
    let path = args.first().ok_or("json: missing file")?;
    let q = open_qvd(path, leniency)?;
    let mut out = io::BufWriter::new(io::stdout().lock());
    let names: Vec<&str> = q.fields().iter().map(|f| f.name.as_str()).collect();
    for row in q.rows() {
        let row = row.map_err(|e| chain(&e))?;
        let mut first = true;
        write!(out, "{{").map_err(|e| e.to_string())?;
        for (name, cell) in names.iter().zip(&row) {
            if !first {
                write!(out, ",").map_err(|e| e.to_string())?;
            }
            first = false;
            write!(out, "\"").map_err(|e| e.to_string())?;
            json_write_str(&mut out, name)?;
            write!(out, "\":").map_err(|e| e.to_string())?;
            write_json_value(&mut out, cell)?;
        }
        writeln!(out, "}}").map_err(|e| e.to_string())?;
    }
    finish(out)
}

fn cmd_rewrite(args: &[String], leniency: &Leniency) -> Result<(), String> {
    let src = args.first().ok_or("rewrite: missing <in>")?;
    let dst = args.get(1).ok_or("rewrite: missing <out>")?;
    let q = open_qvd(src, leniency)?;
    q.to_write_table()
        .and_then(|t| t.write_to_path(dst))
        .map_err(|e| chain(&e))?;
    eprintln!("wrote {}", dst);
    Ok(())
}

fn print_header_row<W: Write>(out: &mut W, q: &Qvd) -> Result<(), String> {
    let mut first = true;
    for f in q.fields() {
        if !first {
            write!(out, "\t").map_err(|e| e.to_string())?;
        }
        first = false;
        write!(out, "{}", f.name).map_err(|e| e.to_string())?;
    }
    writeln!(out).map_err(|e| e.to_string())?;
    Ok(())
}

fn print_value_row<W: Write>(out: &mut W, row: &[Option<Value>]) -> Result<(), String> {
    let mut first = true;
    for cell in row {
        if !first {
            write!(out, "\t").map_err(|e| e.to_string())?;
        }
        first = false;
        match cell {
            None => {}
            Some(Value::Int(i)) => write!(out, "{i}").map_err(|e| e.to_string())?,
            Some(Value::Float(f)) => write!(out, "{f}").map_err(|e| e.to_string())?,
            Some(Value::Str(s)) => {
                write!(out, "{}", s.replace(['\t', '\n'], " ")).map_err(|e| e.to_string())?
            }
            Some(Value::DualInt(d)) => {
                write!(out, "{}", d.text.replace(['\t', '\n'], " ")).map_err(|e| e.to_string())?
            }
            Some(Value::DualFloat(d)) => {
                write!(out, "{}", d.text.replace(['\t', '\n'], " ")).map_err(|e| e.to_string())?
            }
            Some(other) => return Err(format!("unsupported value type: {other:?}")),
        }
    }
    writeln!(out).map_err(|e| e.to_string())?;
    Ok(())
}

fn write_json_value<W: Write>(out: &mut W, cell: &Option<Value>) -> Result<(), String> {
    match cell {
        None => write!(out, "null").map_err(|e| e.to_string())?,
        Some(Value::Int(i)) => write!(out, "{i}").map_err(|e| e.to_string())?,
        Some(Value::Float(f)) => {
            if f.is_finite() {
                write!(out, "{f}").map_err(|e| e.to_string())?
            } else {
                // JSON has no NaN/Infinity; a string keeps them distinct
                // from a real NULL.
                write!(out, "\"{f}\"").map_err(|e| e.to_string())?
            }
        }
        Some(Value::Str(s)) => {
            write!(out, "\"").map_err(|e| e.to_string())?;
            json_write_str(out, s)?;
            write!(out, "\"").map_err(|e| e.to_string())?;
        }
        Some(Value::DualInt(d)) => write!(out, "{}", d.number).map_err(|e| e.to_string())?,
        Some(Value::DualFloat(d)) => {
            if d.number.is_finite() {
                write!(out, "{}", d.number).map_err(|e| e.to_string())?
            } else {
                write!(out, "\"").map_err(|e| e.to_string())?;
                json_write_str(out, &d.text)?;
                write!(out, "\"").map_err(|e| e.to_string())?;
            }
        }
        Some(other) => return Err(format!("unsupported value type: {other:?}")),
    }
    Ok(())
}

fn json_write_str<W: Write>(out: &mut W, s: &str) -> Result<(), String> {
    for c in s.chars() {
        match c {
            '"' => write!(out, "\\\"").map_err(|e| e.to_string())?,
            '\\' => write!(out, "\\\\").map_err(|e| e.to_string())?,
            '\n' => write!(out, "\\n").map_err(|e| e.to_string())?,
            '\r' => write!(out, "\\r").map_err(|e| e.to_string())?,
            '\t' => write!(out, "\\t").map_err(|e| e.to_string())?,
            c if (c as u32) < 0x20 => {
                write!(out, "\\u{:04x}", c as u32).map_err(|e| e.to_string())?
            }
            c => write!(out, "{c}").map_err(|e| e.to_string())?,
        }
    }
    Ok(())
}
