use crate::prepare::{ArchiveType, FileAction, PrepRequirement};
use anyhow::Context;
use reqwest::Url;
use std::path::PathBuf;
use std::process::Command;

const WATDIV_BIN: &str = "data/watdiv/watdiv/bin/Release/watdiv";
const WATDIV_BIN_DIR: &str = "data/watdiv/watdiv/bin/Release";
const WATDIV_MODEL: &str = "../../model/wsdbm-data-model.txt";
const WATDIV_TESTSUITE: &str = "../../testsuite";

pub fn download_watdiv_tarball() -> PrepRequirement {
    PrepRequirement::FileDownload {
        url: Url::parse("https://dsg.uwaterloo.ca/watdiv/watdiv_v06.tar")
            .expect("parse watdiv url"),
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
            if PathBuf::from(WATDIV_BIN).exists() {
                Ok(())
            } else {
                anyhow::bail!(
                    "WatDiv binary not found at {WATDIV_BIN}. Does `make` succeed?"
                );
            }
        }),
    }
}

fn watdiv_bin_absolute() -> anyhow::Result<PathBuf> {
    std::fs::canonicalize(WATDIV_BIN)
        .with_context(|| format!("Failed to resolve watdiv binary at {WATDIV_BIN}. Run `compile_watdiv` first."))
}

fn watdiv_bin_dir_absolute() -> anyhow::Result<PathBuf> {
    std::fs::canonicalize(WATDIV_BIN_DIR)
        .with_context(|| format!("Failed to resolve watdiv bin dir at {WATDIV_BIN_DIR}."))
}

pub fn generate_watdiv_data() -> PrepRequirement {
    PrepRequirement::RunClosure {
        check_requirement: Box::new(|_ctx| {
            if PathBuf::from("data/watdiv/watdiv.nt").exists() {
                Ok(())
            } else {
                anyhow::bail!("WatDiv dataset not found at data/watdiv/watdiv.nt");
            }
        }),
        execute: Box::new(|_ctx| {
            std::fs::create_dir_all("data/watdiv")
                .context("Failed to create data/watdiv/")?;

            let bin = watdiv_bin_absolute()?;
            let bin_dir = watdiv_bin_dir_absolute()?;

            let out_path = std::fs::canonicalize("data/watdiv")
                .context("Failed to resolve data/watdiv/")?
                .join("watdiv.nt");

            let output = Command::new(&bin)
                .current_dir(&bin_dir)
                .args(["-d", WATDIV_MODEL, "100"])
                .output()
                .context("Failed to run watdiv data generator")?;

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                anyhow::bail!("WatDiv data generation failed: {stderr}");
            }

            std::fs::write(&out_path, &output.stdout)
                .context("Failed to write watdiv.nt")?;

            let triple_count = output.stdout.iter().filter(|&&b| b == b'\n').count();
            println!("Generated watdiv.nt ({triple_count} triples)");
            Ok(())
        }),
    }
}

pub fn generate_watdiv_queries() -> PrepRequirement {
    PrepRequirement::RunClosure {
        check_requirement: Box::new(|_ctx| {
            let dir = PathBuf::from("data/watdiv_queries/queries");
            if dir.join("S1.sparql").exists() && dir.join("L5.sparql").exists() {
                Ok(())
            } else {
                anyhow::bail!(
                    "WatDiv queries not found in data/watdiv_queries/queries/"
                );
            }
        }),
        execute: Box::new(|_ctx| {
            let queries_dir = PathBuf::from("data/watdiv_queries/queries");
            std::fs::create_dir_all(&queries_dir)
                .context("Failed to create data/watdiv_queries/queries/")?;

            let bin = watdiv_bin_absolute()?;
            let bin_dir = watdiv_bin_dir_absolute()?;

            let templates = [
                "S1", "S2", "S3", "S4", "S5", "S6", "S7",
                "C1", "C2", "C3",
                "F1", "F2", "F3", "F4", "F5",
                "L1", "L2", "L3", "L4", "L5",
            ];

            for name in templates {
                let template_file = format!("{WATDIV_TESTSUITE}/{name}.txt");
                let out_file = queries_dir.join(format!("{name}.sparql"));

                let output = Command::new(&bin)
                    .current_dir(&bin_dir)
                    .args(["-q", WATDIV_MODEL, &template_file, "1", "1"])
                    .output()
                    .with_context(|| {
                        format!("Failed to run watdiv query generator for {name}")
                    })?;

                if !output.status.success() {
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    anyhow::bail!("WatDiv query generation failed for {name}: {stderr}");
                }

                std::fs::write(&out_file, &output.stdout)
                    .with_context(|| format!("Failed to write {}", out_file.display()))?;

                println!("Generated {name}.sparql");
            }
            Ok(())
        }),
    }
}