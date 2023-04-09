use crate::KvsErr;
use crate::Result;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::fs::{self, File};
use std::io;
use std::io::*;
use std::ops::Range;
use std::path::{Path, PathBuf};

use super::KvEngine;
pub struct KvStore {
    path: PathBuf,
    writer: BufWriterWithPos<File>,                 // 当前写入文件
    version: u64,                                   // 当前版本号
    index: BTreeMap<String, CommandPos>,            // 索引
    readers: BTreeMap<u64, BufReaderWithPos<File>>, // 每个版本文件接口
    uncompacted: u64,                               // 记录需要未被压缩的内容大小
}
const COMPACTION_THRESHOLD: u64 = 1024 * 1024;
#[derive(Debug)]
struct CommandPos {
    version: u64, // key相关的最近一次出现的版本
    pos: u64,     //  key相关的最近一次出现的版本文件位置
    len: u64,     // 指令长度
}
struct BufWriterWithPos<SeekWriter: Write + Seek> {
    writer: BufWriter<SeekWriter>,
    pos: u64,
}
struct BufReaderWithPos<SeekReader: Read + Seek> {
    reader: BufReader<SeekReader>,
    pos: u64,
}
#[derive(Serialize, Deserialize, Debug)]
enum OpCmd {
    Set { key: String, value: String },
    Remove { key: String },
}

fn sorted_version_list(path: &Path) -> Result<Vec<u64>> {
    let mut version_list: Vec<u64> = fs::read_dir(&path)?
        .flat_map(|res| -> Result<_> { Ok(res?.path()) })
        .filter(|path| path.is_file() && path.extension() == Some("log".as_ref()))
        .flat_map(|path| {
            path.file_name()
                .and_then(OsStr::to_str)
                .map(|s| s.trim_end_matches(".log"))
                .map(str::parse::<u64>)
        })
        .flatten()
        .collect();
    version_list.sort_unstable();
    Ok(version_list)
}

// load load version file
fn load(
    version: u64,
    reader: &mut BufReaderWithPos<File>,
    index: &mut BTreeMap<String, CommandPos>,
) -> Result<u64> {
    let mut pos: u64 = reader.seek(SeekFrom::Start(0))?;
    let mut stream = Deserializer::from_reader(reader).into_iter::<OpCmd>();
    let mut uncompacted: u64 = 0;
    while let Some(cmd) = stream.next() {
        let next_pos: u64 = stream.byte_offset() as u64;
        match cmd? {
            OpCmd::Set { key, .. } => {
                // check if there is previous log record
                if let Some(old_cmd) = index.insert(key, (version, pos..next_pos).into()) {
                    uncompacted += old_cmd.len;
                }
            }
            OpCmd::Remove { key } => {
                // check if there is previous log record
                if let Some(old_cmd) = index.remove(&key) {
                    uncompacted += old_cmd.len;
                }
                uncompacted += next_pos - pos;
            }
        }
        pos = next_pos;
    }
    Ok(uncompacted)
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path = path.into();
        fs::create_dir_all(&path)?;
        let mut index = BTreeMap::new();
        let mut readers = BTreeMap::new();
        // load persistent data
        let version_list = sorted_version_list(&path)?;
        let mut uncompacted = 0;
        for &version in &version_list {
            let mut reader = BufReaderWithPos::new(File::open(log_path(&path, version))?)?;
            uncompacted += load(version, &mut reader, &mut index)?;
            readers.insert(version, reader);
        }
        // update version
        let version = version_list.last().unwrap_or(&0) + 1;
        // create new version file
        let writer = new_log_file(&path, version, &mut readers)?;
        let store = KvStore {
            path,
            writer,
            version,
            index,
            readers,
            uncompacted,
        };
        return Ok(store);
    }

    fn add_uncompacted(&mut self, new_uncompacted: u64) -> Result<()> {
        self.uncompacted += new_uncompacted;
        if self.uncompacted > COMPACTION_THRESHOLD {
            self.compact()?
        }

        return Ok(());
    }
    /// Remove a given key.
    fn new_log_file(&mut self, version: u64) -> Result<BufWriterWithPos<File>> {
        new_log_file(&self.path, version, &mut self.readers)
    }
    fn compact(&mut self) -> Result<()> {
        // compact step
        // 1. iterate all keys, copy the latest record to a new file
        // create new log for later write;
        let compaction_version = self.version + 1;
        self.version += 2;
        self.writer = self.new_log_file(self.version)?;

        let mut writer = self.new_log_file(compaction_version)?;
        let mut pos: u64 = 0;
        for cmd_pos in &mut self.index.values_mut() {
            let reader = self
                .readers
                .get_mut(&cmd_pos.version)
                .expect(format!("{} reader not found", cmd_pos.version).as_str());
            if reader.pos != cmd_pos.pos {
                reader.seek(SeekFrom::Start(cmd_pos.pos))?;
            }
            let mut entry_reader = reader.take(cmd_pos.len);
            io::copy(&mut entry_reader, &mut writer)?;
            *cmd_pos = (compaction_version, (pos..writer.pos)).into();
            pos = writer.pos;
        }
        let removed_versions: Vec<_> = self
            .readers
            .keys()
            .filter(|&&version| version < compaction_version)
            .cloned()
            .collect();
        for version in removed_versions {
            self.readers.remove(&version);
            fs::remove_file(log_path(&self.path, version))?;
        }
        self.uncompacted = 0;
        Ok(())
    }
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        return Ok(len);
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.writer.seek(pos)?;
        Ok(self.pos)
    }
}
// new
impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}
// read
impl<R> Read for BufReaderWithPos<R>
where
    R: Read + Seek,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

// seek
impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}
fn log_path(dir: &Path, version: u64) -> PathBuf {
    dir.join(format!("{}.log", version))
}
fn new_log_file(
    path: &Path,
    version: u64,
    readers: &mut BTreeMap<u64, BufReaderWithPos<File>>,
) -> Result<BufWriterWithPos<File>> {
    let path: PathBuf = log_path(path, version);

    // writer file should have rw auth
    let writer = BufWriterWithPos::new(
        std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)?,
    )?;
    readers.insert(version, BufReaderWithPos::new(File::open(&path)?)?);

    Ok(writer)
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((version, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            version,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

impl KvEngine for KvStore {
    fn set(&mut self, key: String, value: String) -> Result<()> {
        // construct data
        let cmd = OpCmd::Set { key, value: value };
        let pos = self.writer.pos;
        // write to file
        serde_json::to_writer(&mut self.writer, &cmd)?;
        self.writer.flush()?;
        // create index for get
        if let OpCmd::Set { key, .. } = cmd {
            if let Some(old_cmd) = self
                .index
                .insert(key, (self.version, pos..self.writer.pos).into())
            {
                // update uncompacted data size
                self.add_uncompacted(old_cmd.len)?;
            }
        }
        Ok(())
    }

    fn get(&mut self, key: String) -> Result<Option<String>> {
        // find last record path from index
        if let Some(cmd) = self.index.get(&key) {
            // println!("get key {} pos {:?}", key, cmd);
            if let Some(reader) = self.readers.get_mut(&cmd.version) {
                reader.seek(SeekFrom::Start(cmd.pos))?;
                let cmd: OpCmd =
                    serde_json::from_reader(reader.take(cmd.len)).expect("fail to serialize");
                match cmd {
                    OpCmd::Set { key: _, value } => {
                        // println!("get key {} , value : {:?}", key, value);
                        return Ok(Some(value));
                    }
                    OpCmd::Remove { key: _ } => {
                        // println!("key {} deleted", key);
                    }
                }
            }
        }
        Ok(None)
    }

    fn remove(&mut self, key: String) -> Result<()> {
        // Check key existence
        if self.index.contains_key(&key) {
            let cmd = OpCmd::Remove { key };
            // serialize record
            serde_json::to_writer(&mut self.writer, &cmd)?;
            self.writer.flush()?;

            if let OpCmd::Remove { key } = cmd {
                if let Some(old_cmd) = self.index.remove(&key) {
                    self.add_uncompacted(old_cmd.len)?
                }
            }
            Ok(())
        } else {
            Err(KvsErr::KeyNotFound)
        }
    }
}