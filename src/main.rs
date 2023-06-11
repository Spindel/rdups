// Std
use std::collections::{BTreeMap, HashMap};
use std::env;
use std::fs::File;
use std::io;
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;
use std::vec::Vec;

// Walkdir
use walkdir::WalkDir;

// Blake3
use blake3::Hasher;

fn main() -> Result<(), io::Error> {
    // Parse arguments.
    let args: Vec<String> = env::args().collect();
    let Some(path) = args.get(1) else {
        println!("Usage rdups DIRECTORY");
        return Ok(());
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
    println!("Group by checksum: {:?}", start.elapsed());

    // Get all duplicated files, grouped by checksum.
    let dups = duplicated_files(group_by_checksum);

    // Print all duplicated files to terminal.
    for (_, files) in dups {
        for path in files {
            println!("{path:?}");
        }
        println!("");
    }

    Ok(())
}

// walk_files, walk all files in all subdirectories.
// Return a vector with size and file path.
fn walk_files(path: &str) -> Result<Vec<(u64, Option<PathBuf>)>, io::Error> {
    let mut files: Vec<(u64, Option<PathBuf>)> = Vec::new();

    for entry in WalkDir::new(path).into_iter().filter_map(|e| e.ok()) {
        if entry.file_type().is_file() {
            let file_len = entry.metadata()?.len();
            if file_len != 0 {
                files.push((file_len, Some(entry.into_path())));
            }
        }
    }
    Ok(files)
}

// group_files_by_size group all files by file size. Using a
// vector with size and path.
fn group_files_by_size(files: Vec<(u64, Option<PathBuf>)>) -> BTreeMap<u64, Vec<Option<PathBuf>>> {
    let mut groups: BTreeMap<u64, Vec<Option<PathBuf>>> = BTreeMap::new();

    for (size, path) in files {
        groups.entry(size).or_default().push(path);
    }
    groups
}

fn filter_file_list(files: BTreeMap<u64, Vec<Option<PathBuf>>>) -> Vec<PathBuf> {
    // Filter the files to check into a list of paths only, flattening the hashmap.
    let files_to_check: Vec<_> = files
        .into_iter()
        .filter(|(_, paths)| paths.len() > 1)
        .map(|(_, paths)| paths.into_iter().flatten())
        .flatten()
        .collect();
    files_to_check
}

fn group_files_worker(
    tx: &mpsc::Sender<(String, PathBuf)>,
    lx: &Arc<Mutex<mpsc::Receiver<PathBuf>>>,
) -> Result<(), io::Error> {
    loop {
        let Ok(rx) = lx.lock() else {
            // eprintln!("Lock poisoned?");
            return Ok(());
        };
        let Ok(path) = rx.recv() else {
            // eprintln!("Sender hung up?");
            return Ok(());
        };
        // Drop the lock guard on rx so other threads can read the channel
        drop(rx);
        // eprintln!("Processing {path:?}");
        let sum = blake3_checksum(&path)?;
        if let Err(_) = tx.send((sum, path)) {
            // Unable to send data to receiever?
            // Main thread may be dead for some reason
            // eprintln!("Send failed?");
            return Ok(());
        };
        // eprintln!("Done one loop");
    }
}

// group_files_by_checksum group all files by checksum. Using blake3 to calculate a
// checksum for the files.
fn group_files_by_checksum(
    files: BTreeMap<u64, Vec<Option<PathBuf>>>,
) -> Result<HashMap<String, Vec<PathBuf>>, io::Error> {
    // Channels for the worker-threads that hash the files
    let (in_tx, in_rx) = mpsc::channel();
    let (out_tx, out_rx) = mpsc::channel();

    // Wrap the readable channel in an mutex with reference counting,
    // so the threads can read them as wanted
    let lx = Arc::new(Mutex::new(in_rx));
    let mut threads = Vec::with_capacity(34);
    for _ in 0..32 {
        let tx = out_tx.clone();
        let rx = lx.clone();
        let t = thread::spawn(move || group_files_worker(&tx, &rx));
        threads.push(t);
    }
    // Drop our tx of result to prevent it from holding our wait-loop alive.
    drop(out_tx);

    // Create a worker thread that posts paths to the workers.
    let filler = thread::spawn(move || {
        let files_to_check = filter_file_list(files);
        for f in files_to_check {
            in_tx.send(f).expect("All worker threads are gone");
        }
        Ok(())
    });
    threads.push(filler);

    let mut groups: HashMap<String, Vec<PathBuf>> = HashMap::new();
    // Consume all hashes from the channel into a HashMap
    while let Ok((sum, path)) = out_rx.recv() {
        groups.entry(sum).or_default().push(path);
    }
    for t in threads {
        t.join().expect("Thread wait error")?;
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
