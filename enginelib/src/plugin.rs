use crate::api::EngineAPI;
use libloading::{Library, Symbol};
use oxifs::OxiFS;
use serde::{Deserialize, Serialize};
use std::mem::ManuallyDrop;
use std::sync::Arc;
use std::{collections::HashMap, fs};
use tracing::{debug, error, info};
#[derive(Clone, Debug)]
pub struct LibraryInstance {
    dynamic_library: Arc<ManuallyDrop<Library>>,
    pub metadata: Arc<LibraryMetadata>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LibraryMetadata {
    pub mod_id: String,
    pub mod_author: String,
    pub rustc_version: String,
    pub api_version: String,
    pub mod_name: String,
    pub mod_version: String,
    pub mod_description: String,
    pub mod_license: String,
    pub mod_credits: String,
    pub mod_dependencies: Vec<LibraryDependency>,
    pub mod_display_url: String,
    pub mod_issue_tracker: String,
    pub mod_server: bool,
}
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LibraryDependency {
    pub mod_git_repo: String,
    pub mod_git_commit: String,
    pub mod_id: String,
}
impl Default for LibraryMetadata {
    fn default() -> Self {
        Self {
            mod_id: String::new(),
            mod_author: String::new(),
            rustc_version: crate::RUSTC_VERSION.to_string(),
            api_version: crate::GIT_VERSION.to_string(),
            mod_name: String::new(),
            mod_version: String::new(),
            mod_description: String::new(),
            mod_license: String::new(),
            mod_credits: String::new(),
            mod_dependencies: Vec::new(),
            mod_display_url: String::new(),
            mod_issue_tracker: String::new(),
            mod_server: false,
        }
    }
}
#[derive(Default, Clone)]
pub struct LibraryManager {
    pub libraries: HashMap<String, LibraryInstance>,
}

impl LibraryManager {
    pub fn drop(self, api: EngineAPI) {
        debug!("Dropping LibraryManager and EngineAPI");
        drop(api);
        drop(self);
    }

    pub fn load_modules(&mut self, api: &mut EngineAPI) {
        let dir_path = "./mods";
        let mut files: Vec<String> = Vec::new();

        info!("Scanning for modules in directory: {}", dir_path);

        match fs::read_dir(dir_path) {
            Ok(entries) => {
                for entry in entries.filter_map(Result::ok) {
                    let path = entry.path();
                    if path.is_file() {
                        if let Some(extension) = path.extension() {
                            if extension == "rf" {
                                debug!("Found valid module file: {}", path.display());
                                files.push(path.display().to_string());
                            }
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to read modules directory {}: {}", dir_path, e);
                return;
            }
        }

        info!("Found {} module(s) to load", files.len());
        for file in files {
            self.load_module(&file, api);
        }
    }

    pub fn load_module(&mut self, path: &str, api: &mut EngineAPI) {
        info!("Loading module from path: {}", path);
        let fs = OxiFS::new(path);

        let tmp_path = fs.tempdir.path();
        #[cfg(target_os = "macos")]
        let library_path = tmp_path.join("mod.dylib");
        #[cfg(all(unix, not(target_os = "macos")))]
        let library_path = tmp_path.join("mod.so");
        #[cfg(windows)]
        let library_path = tmp_path.join("mod.dll");

        if let Some(lib_path_str) = library_path.to_str() {
            debug!("Extracted library path: {}", lib_path_str);
            if let Err(e) = self.load_library(lib_path_str, api) {
                error!("Failed to load library {}: {}", lib_path_str, e);
            }
        } else {
            error!("Invalid library path for module: {}", path);
        }
        //std::mem::forget(fs);
    }

    pub fn load_library(&mut self, path: &str, api: &mut EngineAPI) -> Result<(), String> {
        debug!("Attempting to load library: {}", path);

        let (lib, metadata): (Library, LibraryMetadata) = unsafe {
            match Library::new(path)
                .map_err(|e| format!("Failed to load library: {}", e))
                .and_then(|library| {
                    let metadata_fn: Symbol<unsafe extern "Rust" fn() -> LibraryMetadata> = library
                        .get(b"metadata")
                        .map_err(|e| format!("Failed to load metadata: {}", e))?;
                    let metadata: LibraryMetadata = metadata_fn();
                    Ok((library, metadata))
                }) {
                Ok(result) => result,
                Err(err) => {
                    error!("Failed to load module at {}: {}", path, err);
                    return Err(err);
                }
            }
        };

        // Version compatibility check
        if std::env::var_os("GE_STRICT_MODS").is_some()
            && (metadata.api_version != crate::GIT_VERSION
                || metadata.rustc_version != crate::RUSTC_VERSION)
        {
            let err = format!(
                "Version mismatch - Module API: {}, Engine API: {}, Module Rustc: {}, Engine Rustc: {}",
                metadata.api_version,
                crate::GIT_VERSION,
                metadata.rustc_version,
                crate::RUSTC_VERSION
            );
            error!("{}", err);
            return Err(err);
        }

        // Execute module's run function
        if let Err(e) = unsafe {
            lib.get(b"run")
                .map_err(|e| format!("Failed to get run symbol: {}", e))
                .map(|run: Symbol<unsafe extern "Rust" fn(reg: &mut EngineAPI)>| run(api))
        } {
            error!("Failed to execute module's run function: {}", e);
            return Err(e);
        }

        // Store the loaded library
        self.libraries.insert(
            metadata.mod_id.clone(),
            LibraryInstance {
                dynamic_library: Arc::new(ManuallyDrop::new(lib)),
                metadata: Arc::new(metadata.clone()),
            },
        );

        info!(
            "Successfully loaded module '{}' (version {}) by {}",
            metadata.mod_name, metadata.mod_version, metadata.mod_author
        );
        Ok(())
    }
}
