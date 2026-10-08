use std::process::Command;

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_featherbit-operator"))
}

#[test]
fn version_prints_the_crate_version() {
    let out = bin().arg("version").output().unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert_eq!(
        stdout.trim(),
        format!("featherbit-operator {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn unknown_subcommand_fails() {
    let out = bin().arg("bogus").output().unwrap();
    assert!(!out.status.success());
}
