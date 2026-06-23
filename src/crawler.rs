use std::{ffi::OsStr, path::{Path, PathBuf}};
use walkdir::WalkDir;

fn ext_allowed(ext: Option<&OsStr>, allowed_ext: &[String]) -> bool {
    match ext {
        Some(raw_os_str) => {
            // let clean_ext_str = std::ffi::OsStr::to_str(raw_os_str);
            if let Some(clean_ext_str) = raw_os_str.to_str() {
                for allowed in allowed_ext {
                    if allowed == clean_ext_str {
                        return true;
                    }
                }
            }
            false
        }
        None => false,
    }
}

pub struct DirectoryCrawler {
    root_path: PathBuf,
    allowed_extensions: Vec<String>,
}

impl DirectoryCrawler {
    pub fn new(root: &Path, extensions: Vec<String>) -> Self {
        DirectoryCrawler { root_path: root.to_path_buf(), allowed_extensions: extensions }
    }

    pub fn run(&self) -> Vec<PathBuf> {
        let mut discovered_files: Vec<PathBuf> = Vec::new();

        let walker = WalkDir::new(&self.root_path);
        for entry_result in walker {
            match entry_result {
                Ok(dir_entry) => {
                    let file_path = dir_entry.path();
                    
                    if file_path.is_file() && ext_allowed(file_path.extension(), &self.allowed_extensions) {
                        let owned_path = file_path.to_path_buf();
                        discovered_files.push(owned_path);
                    }
                }
                Err(fs_error) => {
                    eprintln!("Warning: Skipped inaccessible path, Reason: {}", fs_error);
                }
            }
        }
        discovered_files
    }
}



