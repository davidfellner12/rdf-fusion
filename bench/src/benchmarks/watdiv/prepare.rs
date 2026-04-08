use crate::prepare::{ArchiveType, FileAction, PrepRequirement};
use reqwest::Url;
use std::path::{Path, PathBuf};

pub fn download_watdiv_tarball() -> PrepRequirement {
    PrepRequirement::FileDownload {
        url: Url::parse("https://dsg.uwaterloo.ca/watdiv/watdiv_v06.tar")
            .expect("parse watdiv url"),
        file_name: PathBuf::from("watdiv_v06.tar"),
        action: Some(FileAction::Unpack(ArchiveType::Tar)),
    }
}

pub fn compile_watdiv() -> PrepRequirement {
    PrepRequirement::RunCommand {
        workdir: PathBuf::from("watdiv"),
        program: "make".to_string(),
        args: vec![],
        check_requirement: Box::new(|_ctx| {
            if Path::new("watdiv/bin/Release/watdiv").exists() {
                Ok(())
            } else {
                anyhow::bail!("WatDiv binary not found!")
            }
        }),
    }
}

pub fn generate_watdiv_data() -> PrepRequirement {
    PrepRequirement::RunCommand {
        workdir: PathBuf::from("."),
        program: "watdiv/bin/Release/watdiv".to_string(),
        args: vec![
            "-d".to_string(),
            "watdiv/model/wsdbm-data-model.txt".to_string(),
            "10".to_string(),
        ],
        check_requirement: Box::new(|_ctx| {
            if Path::new("watdiv.nt").exists() {
                Ok(())
            } else {
                anyhow::bail!("watdiv.nt not generated yet")
            }
        }),
    }
}

pub fn generate_watdiv_queries() -> PrepRequirement {
    PrepRequirement::RunCommand {
        workdir: PathBuf::from("."),
        program: "watdiv/bin/Release/watdiv".to_string(),
        args: vec![
            "-q".to_string(),
            "watdiv/model/wsdbm-data-model.txt".to_string(),
            "20".to_string(),
            "1".to_string(),
        ],
        check_requirement: Box::new(|_ctx| {
            if Path::new("watdiv_queries").exists() {
                Ok(())
            } else {
                anyhow::bail!("WatDiv queries not generated yet")
            }
        }),
    }
}