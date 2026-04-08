use crate::prepare::{ArchiveType, FileAction, PrepRequirement};
use reqwest::Url;
use std::path::PathBuf;
use std::fs;
use std::path::Path;
use std::process::Command;
use anyhow::{Result, Context};

pub fn download_watdiv() -> PrepRequirement {
    PrepRequirement::FileDownload {
        url: Url::parse("https://dsg.uwaterloo.ca/watdiv/watdiv_v06.tar").unwrap(),
        file_name: PathBuf::from("watdiv.tar"),
        action: Some(FileAction::Unpack(ArchiveType::Tar)),
    }
}

pub fn compile_watdiv() -> PrepRequirement {
    PrepRequirement::RunCommand {
        workdir: PathBuf::from("watdiv"),
        program: "make".to_string(),
        args: vec![],
        check_requirement: Box::new(|_ctx| {
            let bin_path = PathBuf::from("watdiv/bin/watdiv");
            if bin_path.exists() {
                Ok(())
            } else {
                anyhow::bail!("WatDiv binary not found!");
            }
        }),
    }
}

pub fn unpack_watdiv() -> PrepRequirement {
    PrepRequirement::RunClosure {
        execute: Box::new(|_ctx| {
            let status = Command::new("tar")
                .args(["-xf", "watdiv.tar"])
                .status()
                .context("Failed to run tar to unpack WatDiv")?;

            if status.success() {
                Ok(())
            } else {
                anyhow::bail!("Tar process failed to unpack WatDiv")
            }
        }),
        check_requirement: Box::new(|_ctx| {
            if Path::new("watdiv/bin").exists() {
                Ok(())
            } else {
                anyhow::bail!("WatDiv not unpacked yet")
            }
        }),
    }
}