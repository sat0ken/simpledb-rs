use std::fs::{File, OpenOptions};
use std::io::{Read, Write, Seek, SeekFrom};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockId {
    filename: String,
    block_number: usize,
}

impl BlockId {
    pub fn new(filename: String, block_number: usize) -> Self {
        BlockId { filename, block_number }
    }
    pub fn filename(&self) -> &str {
        &self.filename
    }
    pub fn number(&self) -> usize {
        self.block_number
    }
}

pub struct Page {
    data: Vec<u8>,
}

impl Page {
    pub fn new(block_size: usize) -> Self {
        Page { data: vec![0; block_size] }
    }

    pub fn from_bytes(b: &[u8]) -> Self {
        Page { data: b.to_vec() }
    }

    pub fn get_int(&self, offset: usize) -> i32 {
        let bytes = &self.data[offset..offset + 4];
        i32::from_be_bytes(bytes.try_into().unwrap())
    }

    pub fn get_bytes(&self, offset: usize, length: usize) -> Vec<u8> {
        self.data[offset..offset + length].to_vec()
    }

    pub fn get_string(&self, offset: usize, length: usize) -> String {
        String::from_utf8_lossy(&self.data[offset..offset + length]).to_string()
    }

    pub fn set_int(&mut self, offset: usize, val: i32) {
        let bytes = val.to_be_bytes();
        self.data[offset..offset + 4].copy_from_slice(&bytes);
    }

    pub fn set_bytes(&mut self, offset: usize, val: &[u8]) {
        self.data[offset..offset + val.len()].copy_from_slice(val);
    }

    pub fn set_string(&mut self, offset: usize, val: &str) {
        let bytes = val.as_bytes();
        self.data[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    pub fn max_length(strlen: usize) -> usize {
        strlen
    }
}

pub struct FileManager {
    db_directory: String,
    block_size: usize,
}

impl FileManager {
    pub fn new(db_directory: String, block_size: usize) -> Self {
        FileManager { db_directory, block_size }
    }

    pub fn read(&self, blk: &BlockId, page: &mut Page) {
        let path = format!("{}/{}", self.db_directory, blk.filename());
        let mut file = File::open(&path).expect("Failed to open file");
        file.seek(SeekFrom::Start((blk.number() * self.block_size) as u64)).expect("Seek failed");
        file.read_exact(&mut page.data).expect("Failed to read data");
    }

    pub fn write(&self, blk: &BlockId, page: &Page) {
        let path = format!("{}/{}", self.db_directory, blk.filename());
        let mut file = OpenOptions::new().write(true).open(&path).expect("Failed to open file");
        file.seek(SeekFrom::Start((blk.number() * self.block_size) as u64)).expect("Seek failed");
        file.write_all(&page.data).expect("Failed to write data");
    }

    pub fn append(&self, filename: &str) -> BlockId {
        let path = format!("{}/{}", self.db_directory, filename);
        let mut file = OpenOptions::new().append(true).open(&path).expect("Failed to open file");
        let length = file.metadata().expect("Failed to get metadata").len() as usize;
        let new_block_num = length / self.block_size;
        file.set_len(((new_block_num + 1) * self.block_size) as u64).expect("Failed to set file length");
        BlockId::new(filename.to_string(), new_block_num)
    }

    pub fn is_new(&self) -> bool {
        !Path::new(&self.db_directory).exists()
    }

    pub fn length(&self, filename: &str) -> usize {
        let path = format!("{}/{}", self.db_directory, filename);
        let file = File::open(&path).expect("Failed to open file");
        let length = file.metadata().expect("Failed to get metadata").len();
        (length as usize + self.block_size - 1) / self.block_size
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }
}
