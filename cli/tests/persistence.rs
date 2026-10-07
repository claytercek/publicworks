use serde_json::Value;
use std::{
    path::PathBuf,
    process::{Command, Output},
};
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "publicworks-cli-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn invoke(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_publicworks"))
        .args(args)
        .output()
        .unwrap()
}
fn success(args: &[&str]) -> Value {
    let output = invoke(args);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
#[test]
fn create_append_show_across_processes() {
    let dir = Directory::new();
    let path = dir.0.join("demo.db");
    let db = path.to_str().unwrap();
    let created = success(&["create", db]);
    assert_eq!(created["conversationId"], 2);
    assert_eq!(created["commitSeq"], 1);
    let appended = success(&["append", db, "2", "hello durable world"]);
    let entry_id = appended["entryId"].as_u64().unwrap();
    assert!(entry_id > 2); // Reopening may abandon the prior process's ID lease.
    assert_eq!(appended["commitSeq"], 2);
    let shown = success(&["show", db, "2"]);
    assert_eq!(shown["entries"][0]["data"]["text"], "hello durable world");
    assert_eq!(shown["entries"][0]["id"], entry_id);
    for args in [
        ["append", db, "99", "no"].as_slice(),
        ["show", db, "99"].as_slice(),
    ] {
        let failed = invoke(args);
        assert!(!failed.status.success());
        assert!(failed.stdout.is_empty());
        assert!(String::from_utf8_lossy(&failed.stderr).contains("Unknown conversation"));
    }
    assert!(success(&["create", db])["conversationId"].as_u64().unwrap() > entry_id);
    let missing = dir.0.join("missing.db");
    assert!(
        !invoke(&["show", missing.to_str().unwrap(), "1"])
            .status
            .success()
    );
    assert!(!missing.exists());
}
