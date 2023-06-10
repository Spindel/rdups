// Std
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::mpsc;
use std::thread;
use std::vec::Vec;

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

    let (paths_tx, paths_rx) = mpsc::channel();
    let (hash_tx, hash_rx) = mpsc::channel();

    // Make path an owned copy for the walker thread
    let path: String = path.into();

    // Walk all files.
    thread::spawn(move || walk_files(path, paths_tx).expect("Failure is an option"));

    // Split out the filtering file actor, it buffers data from the file walker and then sends
    // paths that should be hashed out as they arrive.
    thread::spawn(move || filter_files(paths_rx, hash_tx));

    // Group all files by checksum.
    let group_by_checksum = group_files_by_checksum(hash_rx)?;

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

/// Filter files as they come in, based on their metadata.
fn filter_files(rx: mpsc::Receiver<(u64, PathBuf)>, tx: mpsc::Sender<PathBuf>) {
    let mut groups: BTreeMap<u64, Vec<Option<PathBuf>>> = BTreeMap::new();
    while let Ok((file_len, path)) = rx.recv() {
        {
            let files = groups.entry(file_len).or_default();
            if files.is_empty() {
                files.push(Some(path));
            } else {
                tx.send(path).unwrap();
                files.push(None);
                /*
                for f in files {
                    if let Some(f) = f.take() {
                        tx.send(f).unwrap();
                    }
                }*/
                // I'm still learning how the iterable approach is supposed to look, this is the
                // same as above
                files
                    .iter_mut()
                    .filter_map(|x| x.take())
                    .for_each(|path| tx.send(path).unwrap());
            }
        }
    }
}

// walk_files, walk all files in all subdirectories.
// Return a vector with size and file path.
fn walk_files(path: String, tx: mpsc::Sender<(u64, PathBuf)>) -> Result<(), io::Error> {
    for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file() {
            let file_len = entry.metadata()?.len();
            if file_len != 0 {
                tx.send((file_len, entry.into_path()))
                    .expect("tx send failure");
            }
        }
    }
    Ok(())
}

// group_files_by_checksum group all files by checksum. Using blake3 to calculate a
// checksum for the files.
fn group_files_by_checksum(
    filechan: mpsc::Receiver<PathBuf>,
) -> Result<HashMap<String, Vec<PathBuf>>, io::Error> {
    use std::sync::{Arc, Mutex};

    // Channels for the worker-threads that hash the files
    let (tx, rx) = mpsc::channel();

    // Wrap the readable channel in an mutex with reference counting,
    // so the threads can read them as wanted
    let lx = Arc::new(Mutex::new(filechan));
    for _ in 0..64 {
        let rx = lx.clone();
        let tx = tx.clone();
        thread::spawn(move || {
            loop {
                let path = {
                    if let Ok(rx) = rx.lock() {
                        rx.recv()
                    } else {
                        // Failed to lock / Poisoned.
                        // Another thread died while holding the rx lock
                        return;
                    }
                };
                if let Ok(path) = path {
                    let sum = blake3_checksum(&path);
                    if tx.send((sum, path)).is_err() {
                        // Unable to send data to receiever?
                        // Main thread may be dead for some reason
                        return;
                    }
                }
                // Channel hung up, no more data
                else {
                    return;
                }
            }
        });
    }
    // Drop the original tx, or we will always have a living sender
    drop(tx);

    let mut groups: HashMap<String, Vec<PathBuf>> = HashMap::new();
    while let Ok((sum, path)) = rx.recv() {
        groups.entry(sum?).or_default().push(path);
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

    let hash = hasher.finalize().to_string();
    Ok(hash)
}
