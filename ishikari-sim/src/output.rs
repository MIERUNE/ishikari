//! Safe publication of simulator artifacts.

use std::{
    fs::{self, File, OpenOptions},
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use anyhow::{Context, Result, bail};

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Existing local archives selected by production source routing for the
/// supplied tileset ids. Missing archives are not inputs and are omitted; this
/// also avoids requiring every parent directory of an intentionally absent
/// tileset to exist merely to validate an unrelated output path.
pub fn local_source_archives_for_tilesets<'a>(
    tileset_sources: &str,
    tileset_ids: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<PathBuf>> {
    ishikari::storage::local_tileset_archive_paths(tileset_sources, tileset_ids)?
        .into_iter()
        .filter_map(|path| match path.try_exists() {
            Ok(true) => Some(Ok(path)),
            Ok(false) => None,
            Err(error) => Some(
                Err(error)
                    .with_context(|| format!("inspect local PMTiles input {}", path.display())),
            ),
        })
        .collect()
}

/// Rejects an output path that names the same destination as any protected
/// input, including lexical aliases, symlinks, and existing hard links.
pub fn ensure_output_distinct<'a>(
    output: &Path,
    protected: impl IntoIterator<Item = &'a Path>,
) -> Result<()> {
    let output_resolved = resolve_destination(output)?;
    for input in protected {
        let input_resolved = resolve_destination(input)?;
        let existing_alias = output
            .try_exists()
            .with_context(|| format!("inspect output path {}", output.display()))?
            && input
                .try_exists()
                .with_context(|| format!("inspect protected path {}", input.display()))?
            && same_file::is_same_file(output, input).with_context(|| {
                format!(
                    "compare output {} with protected path {}",
                    output.display(),
                    input.display()
                )
            })?;
        if output_resolved == input_resolved || existing_alias {
            bail!(
                "output {} must not overwrite input {}",
                output.display(),
                input.display()
            );
        }
    }
    Ok(())
}

/// Local `.pmtiles` archives reachable through a `tileset_sources` value, so
/// callers can protect them from being overwritten by an output artifact.
///
/// A source is a `;`-separated list of `namespace=path` or bare `path` roots
/// (`=` is a namespace separator only when the left side is a valid namespace
/// key, matching production). A `file://` root is treated as its local path;
/// other `scheme://` roots are remote and contribute nothing. Each local root
/// is walked **recursively** for `.pmtiles` files, because production maps a
/// namespaced key like `demo/streets` to the nested path
/// `<root>/demo/streets.pmtiles`. Only existing files are returned, since a
/// not-yet-existing output cannot alias one.
pub fn local_source_archives(tileset_sources: &str) -> Vec<PathBuf> {
    let mut archives = Vec::new();
    for entry in tileset_sources.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let raw = match entry.split_once('=') {
            Some((namespace, path)) if is_namespace_key(namespace.trim()) => path.trim(),
            _ => entry,
        };
        let local = raw.strip_prefix("file://").unwrap_or(raw);
        if local.is_empty() || local.contains("://") {
            continue;
        }
        collect_pmtiles(Path::new(local), &mut archives);
    }
    archives
}

/// Recursively collects `.pmtiles` files under `dir`. Symlinked directories are
/// not traversed, so a symlink cycle cannot loop.
fn collect_pmtiles(dir: &Path, archives: &mut Vec<PathBuf>) {
    let Ok(read_dir) = fs::read_dir(dir) else {
        return;
    };
    for item in read_dir.flatten() {
        let Ok(file_type) = item.file_type() else {
            continue;
        };
        let path = item.path();
        if file_type.is_dir() {
            collect_pmtiles(&path, archives);
        } else if file_type.is_file()
            && path
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("pmtiles"))
        {
            archives.push(path);
        }
    }
}

fn is_namespace_key(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

/// A buffered output created beside its final destination. Finishing flushes
/// and syncs the file, then atomically renames it into place. Dropping before a
/// successful finish removes the temporary file and leaves the destination
/// untouched.
pub struct AtomicOutputFile {
    writer: Option<BufWriter<File>>,
    temporary_path: PathBuf,
    destination_path: PathBuf,
    published: bool,
}

impl AtomicOutputFile {
    pub fn create(destination: &Path) -> Result<Self> {
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let (file, temporary_path) = create_temporary_file(parent)
            .with_context(|| format!("create temporary output beside {}", destination.display()))?;
        Ok(Self {
            writer: Some(BufWriter::new(file)),
            temporary_path,
            destination_path: destination.to_path_buf(),
            published: false,
        })
    }

    pub fn writer(&mut self) -> &mut BufWriter<File> {
        self.writer
            .as_mut()
            .expect("atomic output writer is available before finish")
    }

    pub fn finish(mut self) -> Result<()> {
        let mut writer = self
            .writer
            .take()
            .expect("atomic output writer is finished once");
        writer
            .flush()
            .with_context(|| format!("flush temporary output {}", self.temporary_path.display()))?;
        writer
            .get_ref()
            .sync_all()
            .with_context(|| format!("sync temporary output {}", self.temporary_path.display()))?;
        drop(writer);
        fs::rename(&self.temporary_path, &self.destination_path).with_context(|| {
            format!(
                "publish output {} as {}",
                self.temporary_path.display(),
                self.destination_path.display()
            )
        })?;
        self.published = true;
        Ok(())
    }
}

impl Drop for AtomicOutputFile {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.temporary_path);
        }
    }
}

/// Writes a complete artifact without exposing a truncated or partially
/// serialized destination.
pub fn write_atomic(
    destination: &Path,
    write: impl FnOnce(&mut BufWriter<File>) -> Result<()>,
) -> Result<()> {
    let mut output = AtomicOutputFile::create(destination)?;
    write(output.writer())?;
    output.finish()
}

fn resolve_destination(path: &Path) -> Result<PathBuf> {
    match fs::canonicalize(path) {
        Ok(path) => Ok(path),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let file_name = path
                .file_name()
                .with_context(|| format!("output path has no file name: {}", path.display()))?;
            let parent = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let parent = fs::canonicalize(parent)
                .with_context(|| format!("resolve output directory {}", parent.display()))?;
            Ok(parent.join(file_name))
        }
        Err(error) => Err(error).with_context(|| format!("resolve path {}", path.display())),
    }
}

fn create_temporary_file(parent: &Path) -> Result<(File, PathBuf)> {
    for _ in 0..100 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = parent.join(format!(
            ".ishikari-sim-{}-{sequence}.tmp",
            std::process::id()
        ));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => return Ok((file, path)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    bail!("could not allocate a unique temporary output file")
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    fn test_dir(label: &str) -> PathBuf {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ishikari-sim-output-{label}-{}-{sequence}",
            std::process::id()
        ));
        fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn rejects_lexical_symlink_and_hard_link_aliases() {
        let dir = test_dir("aliases");
        let input = dir.join("trace.jsonl");
        fs::write(&input, b"trace").unwrap();

        assert!(ensure_output_distinct(&dir.join("./trace.jsonl"), [input.as_path()]).is_err());

        #[cfg(unix)]
        {
            let symlink = dir.join("trace-link.jsonl");
            std::os::unix::fs::symlink(&input, &symlink).unwrap();
            assert!(ensure_output_distinct(&symlink, [input.as_path()]).is_err());

            let hard_link = dir.join("trace-hard-link.jsonl");
            fs::hard_link(&input, &hard_link).unwrap();
            assert!(ensure_output_distinct(&hard_link, [input.as_path()]).is_err());
        }

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn source_archives_are_enumerated_and_protected_from_output() {
        let dir = test_dir("archives");
        let source = dir.join("data");
        fs::create_dir_all(&source).unwrap();
        let archive = source.join("japan.pmtiles");
        fs::write(&archive, b"PMTiles").unwrap();
        fs::write(source.join("notes.txt"), b"ignore").unwrap();

        // A nested archive (production maps `demo/streets` -> demo/streets.pmtiles).
        let nested_dir = source.join("demo");
        fs::create_dir_all(&nested_dir).unwrap();
        let nested = nested_dir.join("streets.pmtiles");
        fs::write(&nested, b"PMTiles").unwrap();

        let sources = source.to_string_lossy().into_owned();
        let mut archives = local_source_archives(&sources);
        archives.sort();
        let mut expected = vec![archive.clone(), nested.clone()];
        expected.sort();
        assert_eq!(
            archives, expected,
            "must find both top-level and nested archives"
        );

        // The nested archive is protected too — the previous immediate-only scan missed it.
        let protected: Vec<_> = archives.iter().map(PathBuf::as_path).collect();
        assert!(ensure_output_distinct(&nested, protected.clone()).is_err());
        assert!(ensure_output_distinct(&archive, protected.clone()).is_err());
        // A distinct output beside them is allowed.
        assert!(ensure_output_distinct(&source.join("sweep.jsonl"), protected).is_ok());

        // A `file://` root resolves to the same local archives.
        assert!(!local_source_archives(&format!("file://{sources}")).is_empty());
        // A namespaced local root is parsed like production.
        assert!(!local_source_archives(&format!("basemaps={sources}")).is_empty());
        // Remote and empty roots contribute nothing.
        assert!(local_source_archives("gs://bucket/tiles").is_empty());
        assert!(local_source_archives("").is_empty());

        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn exact_source_resolution_protects_archives_below_symlinked_directories() {
        let dir = test_dir("exact-archives");
        let source = dir.join("source with space");
        let outside = dir.join("outside");
        fs::create_dir_all(&source).unwrap();
        fs::create_dir_all(&outside).unwrap();
        let archive = outside.join("streets.pmtiles");
        fs::write(&archive, b"PMTiles").unwrap();
        std::os::unix::fs::symlink(&outside, source.join("demo")).unwrap();

        let source_url = reqwest::Url::from_directory_path(&source).expect("file URL");
        assert!(source_url.as_str().contains("%20"));
        let protected = local_source_archives_for_tilesets(source_url.as_str(), ["demo/streets"])
            .expect("production source resolution");
        assert_eq!(protected, [source.join("demo/streets.pmtiles")]);
        assert!(
            ensure_output_distinct(&archive, protected.iter().map(PathBuf::as_path)).is_err(),
            "the output must not replace an archive reached through a symlinked directory"
        );
        assert!(
            local_source_archives_for_tilesets(source_url.as_str(), ["missing/archive"])
                .expect("missing archives are ignored")
                .is_empty()
        );

        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn failed_atomic_write_preserves_the_previous_destination() {
        let dir = test_dir("atomic");
        let destination = dir.join("report.json");
        fs::write(&destination, b"old").unwrap();

        let error = write_atomic(&destination, |writer| {
            writer.write_all(b"partial")?;
            bail!("synthetic serialization failure")
        })
        .unwrap_err();
        assert!(error.to_string().contains("synthetic"));
        assert_eq!(fs::read(&destination).unwrap(), b"old");

        write_atomic(&destination, |writer| {
            writer.write_all(b"complete")?;
            Ok(())
        })
        .unwrap();
        assert_eq!(fs::read(&destination).unwrap(), b"complete");

        fs::remove_dir_all(dir).unwrap();
    }
}
