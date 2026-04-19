//! Semantic round-trip validator: for every QVD under a directory,
//! read it, re-serialise via the writer, re-read the bytes, and verify
//! cell-by-cell equivalence.
//!
//! Usage:
//!     cargo run --release --example round_trip -- <dir> [--max-bytes N]
//!
//! Skips LFS pointers, misnamed CSV/QVS, and files larger than
//! `--max-bytes` (default 64 MiB) to keep memory bounded.

use std::fs;
use std::path::{Path, PathBuf};

use openqvd::{Qvd, Value};

fn main() {
    let mut args = std::env::args().skip(1);
    let dir = args
        .next()
        .unwrap_or_else(|| "/workspaces/Sigilweaver/QVD-Sources/downloads".into());
    let mut max_bytes: u64 = 64 * 1024 * 1024;
    while let Some(a) = args.next() {
        if a == "--max-bytes" {
            if let Some(v) = args.next() {
                if let Ok(n) = v.parse() {
                    max_bytes = n;
                }
            }
        }
    }

    let mut paths: Vec<PathBuf> = Vec::new();
    collect(Path::new(&dir), &mut paths);
    paths.sort();

    let mut total = 0usize;
    let mut ok = 0usize;
    let mut skipped = 0usize;
    let mut oversize = 0usize;
    let mut fail = 0usize;
    let mut failures: Vec<(PathBuf, String)> = Vec::new();

    for p in &paths {
        total += 1;
        let size = fs::metadata(p).map(|m| m.len()).unwrap_or(0);
        if size > max_bytes {
            oversize += 1;
            continue;
        }
        match try_one(p) {
            Ok(Outcome::Ok) => ok += 1,
            Ok(Outcome::Skip) => skipped += 1,
            Err(e) => {
                fail += 1;
                if failures.len() < 20 {
                    failures.push((p.clone(), e));
                }
            }
        }
    }

    println!(
        "round-trip: total={} ok={} skipped={} oversize={} fail={}",
        total, ok, skipped, oversize, fail
    );
    for (p, reason) in &failures {
        println!("  FAIL {} :: {}", p.display(), reason);
    }
}

enum Outcome {
    Ok,
    Skip,
}

fn collect(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(rd) = fs::read_dir(dir) else { return };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            collect(&p, out);
        } else if p
            .extension()
            .and_then(|x| x.to_str())
            .map(|x| x.eq_ignore_ascii_case("qvd"))
            .unwrap_or(false)
        {
            out.push(p);
        }
    }
}

fn try_one(p: &Path) -> Result<Outcome, String> {
    let bytes = fs::read(p).map_err(|e| format!("read: {e}"))?;
    if bytes.starts_with(b"version ") {
        return Ok(Outcome::Skip);
    }
    if !looks_like_xml(&bytes[..bytes.len().min(64)]) {
        return Ok(Outcome::Skip);
    }

    let a = Qvd::from_bytes(bytes).map_err(|e| format!("read A: {e}"))?;
    let wt = a.to_write_table();
    let rewritten = wt.to_bytes().map_err(|e| format!("write: {e}"))?;
    drop(wt);
    let b = Qvd::from_bytes(rewritten).map_err(|e| format!("read B: {e}"))?;

    if a.num_rows() != b.num_rows() {
        return Err(format!("row count: {} != {}", a.num_rows(), b.num_rows()));
    }
    if a.fields().len() != b.fields().len() {
        return Err("field count mismatch".into());
    }
    for (fa, fb) in a.fields().iter().zip(b.fields()) {
        if fa.name != fb.name {
            return Err(format!("field name: {:?} != {:?}", fa.name, fb.name));
        }
    }

    // Stream row-by-row; never buffer all rows.
    let mut ia = a.rows();
    let mut ib = b.rows();
    let mut idx = 0usize;
    loop {
        match (ia.next(), ib.next()) {
            (None, None) => break,
            (Some(ra), Some(rb)) => {
                for (j, (ca, cb)) in ra.iter().zip(&rb).enumerate() {
                    if !cells_equal(ca, cb) {
                        return Err(format!(
                            "row {idx} col {j} ({:?}): {:?} vs {:?}",
                            a.fields()[j].name,
                            ca,
                            cb
                        ));
                    }
                }
                idx += 1;
            }
            _ => return Err("iterator length mismatch".into()),
        }
    }
    Ok(Outcome::Ok)
}

fn looks_like_xml(b: &[u8]) -> bool {
    b.starts_with(b"<?xml") || b.starts_with(b"<QvdTableHeader")
}

fn cells_equal(a: &Option<Value>, b: &Option<Value>) -> bool {
    match (a, b) {
        (None, None) => true,
        (Some(x), Some(y)) => value_eq(x, y),
        _ => false,
    }
}

fn value_eq(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(x), Value::Int(y)) => x == y,
        (Value::Float(x), Value::Float(y)) => x.to_bits() == y.to_bits(),
        (Value::Str(x), Value::Str(y)) => x == y,
        (Value::DualInt(x), Value::DualInt(y)) => x.number == y.number && x.text == y.text,
        (Value::DualFloat(x), Value::DualFloat(y)) => {
            x.number.to_bits() == y.number.to_bits() && x.text == y.text
        }
        _ => false,
    }
}
