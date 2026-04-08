use crate::prepare::actions::ArchiveType;
use anyhow::{Context, Error};
use bzip2::read::MultiBzDecoder;
use std::fs;
use std::fs::File;
use std::io::{Cursor, Read};
use std::path::Path;
use zip::ZipArchive;

pub fn unpack_archive(file_path: &Path, archive_type: ArchiveType) -> Result<(), Error> {
    println!("Unpacking file ...");

    match archive_type {
        ArchiveType::Bz2 => {
            let mut buf = Vec::new();
            MultiBzDecoder::new(File::open(file_path)?).read_to_end(&mut buf)?;
            fs::write(file_path, &buf)?;
        }
        ArchiveType::Zip => {
            let archive = fs::read(file_path).context("Cannot read zip file")?;
            fs::remove_file(file_path).context("Cannot remove existing .zip file")?;
            ZipArchive::new(Cursor::new(archive))
                .context("Invalid .zip file")?
                .extract_unwrapped_root_dir(file_path, |_| true)
                .context("Cannot extract zip file")?;
        }
        ArchiveType::Tar => {
            let tar_file = File::open(file_path).context("Cannot open tar file")?;
            let mut archive = tar::Archive::new(tar_file);
            let parent_dir = file_path.parent().unwrap_or(Path::new("."));
            archive
                .unpack(parent_dir)
                .context("Failed to unpack tar file")?;
        }
    }

    println!("File unpacked.");

    Ok(())
}
