//! Command-line tool output.

use std::process::Command;

use openqvd::{Column, Dual, Value, WriteTable};

fn run(args: &[&str]) -> (bool, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_openqvd"))
        .args(args)
        .output()
        .unwrap();
    (
        out.status.success(),
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
    )
}

fn sample(dir: &std::path::Path) -> String {
    let t = WriteTable::new(
        "t",
        vec![
            Column::new(
                "f",
                vec![
                    Some(Value::Float(1.5)),
                    Some(Value::Float(f64::NAN)),
                    Some(Value::Float(f64::INFINITY)),
                    Some(Value::Float(f64::NEG_INFINITY)),
                    None,
                ],
            ),
            Column::new(
                "d",
                vec![
                    Some(Value::DualFloat(Dual::new(f64::NAN, "n/a"))),
                    None,
                    None,
                    None,
                    None,
                ],
            ),
        ],
    )
    .unwrap();
    let p = dir.join("s.qvd");
    t.write_to_path(&p).unwrap();
    p.to_str().unwrap().to_owned()
}

#[test]
fn json_keeps_non_finite_floats_distinct_from_null() {
    let dir = tempdir();
    let path = sample(&dir);
    let (ok, out, _) = run(&["json", &path]);
    assert!(ok);
    let lines: Vec<_> = out.lines().collect();
    assert_eq!(lines[0], r#"{"f":1.5,"d":"n/a"}"#);
    assert_eq!(lines[1], r#"{"f":"NaN","d":null}"#);
    assert_eq!(lines[2], r#"{"f":"inf","d":null}"#);
    assert_eq!(lines[3], r#"{"f":"-inf","d":null}"#);
    assert_eq!(lines[4], r#"{"f":null,"d":null}"#);
}

#[test]
fn errors_print_the_cause_and_exit_nonzero() {
    let (ok, _, err) = run(&["csv", "/definitely/not/here.qvd"]);
    assert!(!ok);
    assert!(err.starts_with("error: io error"), "{err}");
}

fn tempdir() -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("openqvd-cli-{}", std::process::id()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[cfg(target_os = "linux")]
#[test]
fn a_failed_write_exits_nonzero() {
    let path = sample(&tempdir());
    for cmd in ["csv", "json", "stat", "head"] {
        let out = Command::new(env!("CARGO_BIN_EXE_openqvd"))
            .args([cmd, &path])
            .stdout(
                std::fs::OpenOptions::new()
                    .write(true)
                    .open("/dev/full")
                    .unwrap(),
            )
            .output()
            .unwrap();
        assert!(!out.status.success(), "{cmd} exited 0 with a full disk");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("writing output"),
            "{cmd}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}

fn date_text_file(dir: &std::path::Path) -> String {
    use openqvd::NumberFormat;
    let mut c = Column::new(
        "d",
        vec![Some(Value::Float(45_000.0)), Some(Value::Str("N/A".into()))],
    );
    c.number_format = NumberFormat::new("DATE");
    let p = dir.join("date_text.qvd");
    WriteTable::new("t", vec![c])
        .unwrap()
        .write_to_path(&p)
        .unwrap();
    p.to_str().unwrap().to_owned()
}

#[test]
fn lenient_flags_reach_the_reader() {
    let path = date_text_file(&tempdir());
    let (ok, _, err) = run(&["csv", &path]);
    assert!(!ok);
    assert!(err.contains("non_numeric_temporal"), "{err}");
    for flags in [
        vec!["csv", "--lenient", &path],
        vec!["csv", &path, "--allow", "non_numeric_temporal"],
    ] {
        let (ok, out, err) = run(&flags);
        assert!(ok, "{flags:?}: {err}");
        assert!(out.contains("N/A"), "{out}");
        assert!(err.contains("warning: non_numeric_temporal"), "{err}");
    }
    let (ok, _, err) = run(&["csv", "--allow", "bogus", &path]);
    assert!(!ok);
    assert!(err.contains("unknown lenient check"), "{err}");
}
