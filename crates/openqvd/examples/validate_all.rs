//! Bulk validation: walk a directory, try to parse every `.qvd` file, and
//! report errors. Used during reverse engineering; not shipped.

use std::env;
use std::path::Path;
use std::process::ExitCode;

use openqvd::{Qvd, ReadOptions};

fn walk(dir: &Path, out: &mut Vec<std::path::PathBuf>) {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return;
    };
    for e in rd.flatten() {
        let p = e.path();
        if p.is_dir() {
            walk(&p, out);
        } else if p.extension().and_then(|s| s.to_str()) == Some("qvd") {
            out.push(p);
        }
    }
}

fn main() -> ExitCode {
    let Some(root) = env::args_os().nth(1) else {
        eprintln!("usage: cargo run --release --example validate_all -- <dir> [--lenient]");
        return ExitCode::from(2);
    };
    let root = std::path::PathBuf::from(root);
    let lenient = env::args_os().any(|a| a == "--lenient");
    let options = if lenient {
        ReadOptions::new().with_leniency(openqvd::Leniency::lenient())
    } else {
        ReadOptions::default()
    };
    let mut warned = 0usize;
    let mut files = Vec::new();
    walk(&root, &mut files);

    let mut ok = 0usize;
    let mut fail = 0usize;
    let mut skipped = 0usize;
    let mut first_fails: Vec<(std::path::PathBuf, String)> = Vec::new();
    for p in &files {
        // Skip CSV / QVS files masquerading as .qvd and LFS pointers.
        let mut buf = [0u8; 40];
        let n = match std::fs::File::open(p).and_then(|mut f| {
            use std::io::Read;
            f.read(&mut buf)
        }) {
            Ok(n) => n,
            Err(_) => {
                skipped += 1;
                continue;
            }
        };
        let head = &buf[..n];
        if !(head.starts_with(b"<?xml") || head.starts_with(b"<QvdTableHeader")) {
            skipped += 1;
            continue;
        }

        // Open, then decode every row: a corrupt symbol index only shows up
        // when the rows are read.
        let res = Qvd::open(p, &options).and_then(|q| {
            if !q.warnings().is_empty() {
                warned += 1;
                for w in q.warnings() {
                    println!("WARN {}: {w}", p.display());
                }
            }
            for row in q.rows() {
                row?;
            }
            Ok(())
        });
        match res {
            Ok(()) => ok += 1,
            Err(e) => {
                fail += 1;
                if first_fails.len() < 20 {
                    first_fails.push((p.clone(), e.to_string()));
                }
            }
        }
    }
    println!(
        "total={} ok={ok} fail={fail} skipped={skipped} with_warnings={warned}",
        files.len()
    );
    for (p, e) in &first_fails {
        println!("  FAIL {}: {e}", p.display());
    }
    if fail > 0 {
        ExitCode::FAILURE
    } else {
        ExitCode::SUCCESS
    }
}
