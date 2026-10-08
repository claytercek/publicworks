use std::{fs, process::Command};

#[test]
fn temporary_databases_are_scoped_and_explicitly_retained() {
    let root = tempfile::tempdir().unwrap();
    for keep in [false, true] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_publicworks-perf"));
        command
            .args(["--profile", "quick"])
            .env("TMPDIR", root.path())
            .env("TMP", root.path())
            .env("TEMP", root.path())
            .env_remove("PUBLICWORKS_PERF_KEEP_DB");
        if keep {
            command.env("PUBLICWORKS_PERF_KEEP_DB", "1");
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).starts_with("profile\tadapter\t"));
        let directories = fs::read_dir(root.path())
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .collect::<Vec<_>>();
        if keep {
            assert_eq!(directories.len(), 5);
            let diagnostics = String::from_utf8_lossy(&output.stderr);
            for directory in directories {
                assert!(directory.join("workload.sqlite").is_file());
                assert!(diagnostics.contains(directory.to_str().unwrap()));
            }
        } else {
            assert!(directories.is_empty(), "workload directories leaked");
            assert!(output.stderr.is_empty());
        }
    }
}
