//! Small CLI that runs the reader against one or more `.qvd` files and
//! prints a summary. Used during reverse engineering as a sanity check;
//! not a production tool.

use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

use openqvd::{Qvd, Value};

fn main() -> ExitCode {
    let args: Vec<PathBuf> = env::args_os().skip(1).map(Into::into).collect();
    if args.is_empty() {
        eprintln!("usage: cargo run --example dump -- <path.qvd> [more.qvd ...]");
        return ExitCode::from(2);
    }
    let mut ok = 0usize;
    let mut fail = 0usize;
    for path in args {
        match Qvd::from_path(&path) {
            Ok(qvd) => {
                ok += 1;
                println!(
                    "OK  {}  table={:?} fields={} rows={}",
                    path.display(),
                    qvd.table_name(),
                    qvd.fields().len(),
                    qvd.num_rows()
                );
                for (i, f) in qvd.fields().iter().enumerate().take(5) {
                    println!(
                        "    field[{i}] {:?} bits@{}+{} bias={} n_sym={} type={:?}",
                        f.name, f.bit_offset, f.bit_width, f.bias, f.no_of_symbols, f.number_format_type
                    );
                }
                for (ri, row) in qvd.rows().take(3).enumerate() {
                    print!("    row {ri}:");
                    for cell in &row {
                        match cell {
                            None => print!(" NULL"),
                            Some(Value::Int(v)) => print!(" int({v})"),
                            Some(Value::Float(v)) => print!(" f64({v})"),
                            Some(Value::Str(s)) => print!(" str({s:?})"),
                            Some(Value::DualInt(d)) => print!(" di({},{:?})", d.number, d.text),
                            Some(Value::DualFloat(d)) => print!(" df({},{:?})", d.number, d.text),
                        }
                    }
                    println!();
                }
            }
            Err(e) => {
                fail += 1;
                eprintln!("FAIL {}: {e}", path.display());
            }
        }
    }
    println!("\nsummary: ok={ok} fail={fail}");
    if fail > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
