// Std
use std::collections::HashMap;
use std::env;
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::vec::Vec;
use std::time::Instant;

// Walkdir
use walkdir::WalkDir;

// Blake3
use blake3::Hasher;

fn main() -> Result<(), io::Error> {
    // Parse arguments.
    let args: Vec<String> = env::args().collect();
    let path = match args.get(1) {
        Some(path) => path,
        None => {
            println!("Usage rdups DIRECTORY");
            return Ok(());
        }
    };

    // Walk all files.
    let start = Instant::now();
    let files = walk_files(path)?;
    println!("walk files: {:?}", start.elapsed());

    // Group all files by size.
    let start = Instant::now();
    let group_by_size = group_files_by_size(files);
    println!("group by size: {:?}", start.elapsed());

    // Group all files by checksum.
    let start = Instant::now();
    let group_by_checksum = group_files_by_checksum(group_by_size)?;
    println!("group by checksum: {:?}", start.elapsed());

    // Get all duplicated files, grouped by checksum.
    let dups = duplicated_files(group_by_checksum);

    // Print all duplicated files to terminal.
    for (_, files) in dups {
        for path in files {
            println!("{:?}", path);
        }
        println!("");
    }

    Ok(())
}

// walk_files, walk all files in all subdirectories.
// Return a vector with size and file path.
fn walk_files(path: &str) -> Result<Vec<(u64, PathBuf)>, io::Error> {
    let mut files: Vec<(u64, PathBuf)> = Vec::new();

    for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file() {
            let file = File::open(entry.path())?;
            let file_metadata = file.metadata()?;

            if file_metadata.len() != 0 {
                files.push((file_metadata.len(), entry.path().to_path_buf()));
            }
        }
    }
    Ok(files)
}

// group_files_by_size group all files by file size. Using a
// vector with size and path.
fn group_files_by_size(files: Vec<(u64, PathBuf)>) -> HashMap<u64, Vec<PathBuf>> {
    let mut groups: HashMap<u64, Vec<PathBuf>> = HashMap::new();

    for (size, path) in files {
        groups.entry(size).or_default().push(path);
    }
    groups
}

// group_files_by_checksum group all files by checksum. Using blake3 to calculate a
// checksum for the files.
fn group_files_by_checksum(
    files: HashMap<u64, Vec<PathBuf>>,
) -> Result<HashMap<String, Vec<PathBuf>>, io::Error> {
    let mut groups: HashMap<String, Vec<PathBuf>> = HashMap::new();

    for (_, files) in files {
        if files.len() > 1 {
            for path in files {
                let sum = blake3_checksum(&path)?;
                groups.entry(sum).or_default().push(path);
            }
        }
    }
    Ok(groups)
}

// duplicated_files check if the HashMap with checksum and files,
// has more then one file in vector. If more then one, its a duplicated file.
fn duplicated_files(files: HashMap<String, Vec<PathBuf>>) -> HashMap<String, Vec<PathBuf>> {
    let mut dups: HashMap<String, Vec<PathBuf>> = HashMap::new();

    for (sum, files) in files {
        if files.len() > 1 {
            for path in files {
                dups.entry(sum.clone()).or_default().push(path);
            }
        }
    }
    dups
}

// blake3_checksum read file, get BLAKE3 checksum.
fn blake3_checksum(path: &PathBuf) -> Result<String, io::Error> {
    // Open file.
    let mut f = File::open(path)?;

    // Create a new BLAKE3, copy, then read checksum.
    let mut hasher = Hasher::new();
    let _ = io::copy(&mut f, &mut hasher);

    Ok(format!("{}", hasher.finalize().to_string()))
}
