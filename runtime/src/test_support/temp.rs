use std::path::{Path, PathBuf};

/// A uniquely named temporary directory containing one database path.
pub struct TempDatabase {
    _directory: tempfile::TempDir,
    path: PathBuf,
}

impl AsRef<Path> for TempDatabase {
    fn as_ref(&self) -> &Path {
        self.path()
    }
}

impl TempDatabase {
    pub fn new(prefix: &str, file_name: &str) -> Self {
        let directory = tempfile::Builder::new()
            .prefix(prefix)
            .tempdir()
            .expect("create temporary database directory");
        let path = directory.path().join(file_name);
        Self {
            _directory: directory,
            path,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}
