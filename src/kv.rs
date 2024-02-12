#![allow(unused_imports)]
#![allow(dead_code)]
use crate::{KvsError, Result};
use std::{
    collections::HashMap,
    io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    ops::Range,
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

pub struct KvStore {
    // directory for the log and other data
    path: PathBuf,
}

#[derive(Serialize, Deserialize, Debug)]
enum Command {
    Set { key: String, value: String },
    Get { key: String },
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::Set { key, value }
    }
    fn get(key: String) -> Command {
        Command::Get { key }
    }
}

struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    buf: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(Self {
            buf: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, _buf: &mut [u8]) -> std::io::Result<usize> {
        todo!()
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, _pos: std::io::SeekFrom) -> std::io::Result<u64> {
        todo!()
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    buf: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(Self {
            buf: BufWriter::new(inner),
            pos,
        })
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, _buf: &[u8]) -> std::io::Result<usize> {
        todo!()
    }
    fn flush(&mut self) -> std::io::Result<()> {
        todo!()
    }
}

impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
    fn seek(&mut self, _pos: SeekFrom) -> std::io::Result<u64> {
        todo!()
    }
}
