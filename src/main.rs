use crate::sql_engine::SqlEngine;
use anyhow::{bail, Context, Result};
use std::fmt::Display;
use std::fs::File;
use std::io::{prelude::*, SeekFrom};
use std::path::PathBuf;

mod lexer;
mod parser;
mod sql_engine;

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
    let command = &args[2];

    let first_char = command.chars().next().unwrap();
    let rest = command.chars().skip(1).collect::<String>();

    match first_char {
        '.' => handle_dot_command(&rest, &args[1..])?,
        _ => run_sql_command(&args[1..])?,
    }

    Ok(())
}

pub struct Db {
    file: File,
    header: DbHeader,
    master_page: DbPage<TableLeafRecord>,
}

impl Db {
    fn new(path: PathBuf) -> Self {
        let mut file = File::open(path).unwrap();
        let header = DbHeader::parse(&mut file);
        let master_page = DbPage::parse_master(&mut file);

        Self {
            file,
            header,
            master_page,
        }
    }

    fn run_sql_command(&mut self, command: &str) {
        let sql_engine = SqlEngine::new();

        // Codecrafters input doesn't include a semicolon, so lets add one.
        if !command.ends_with(';') {
            self.run_sql_command(&format!("{};", command));
        } else {
            sql_engine.execute(command, self);
        }
    }

    fn get_table(&mut self, table_name: &str) -> Table {
        dbg!(&self.master_page.records);
        let table = self
            .master_page
            .records
            .iter()
            .find(|record| {
                let table = dbg!(Table::parse(record));
                table.name.to_ascii_lowercase() == table_name.to_ascii_lowercase()
            })
            .unwrap();

        Table::parse(table)
    }

    fn load_table(&mut self, table: &Table) -> DbPage<TableLeafRecord> {
        eprintln!("### TRYING TO LOAD TABLE {} ###", table.name);
        let offset = dbg!((table.root_page as u64 - 1) * self.header.page_size as u64);
        DbPage::parse(&mut self.file, offset)
    }
}

fn run_sql_command(args: &[String]) -> Result<()> {
    let path = PathBuf::from(&args[0]);
    let mut db = Db::new(path);
    db.run_sql_command(&args[1]);

    Ok(())
}

// TODO: USE DB HERE!
fn handle_dot_command(command: &str, args: &[String]) -> Result<()> {
    let path = PathBuf::from(&args[0]);
    let mut file = File::open(path).context("Failed to open database file")?;
    let header = DbHeader::parse(&mut file);
    let master_page = DbPage::parse_master(&mut file);

    match command {
        "dbinfo" => {
            eprintln!("version: {:x}", header.sqlite_version_number);
            println!("database page size: {}", header.page_size);

            println!("number of tables: {}", master_page.header.cell_count);
        }
        "tables" => {
            println!("number of tables: {}", master_page.header.cell_count);

            let table_names = master_page.records.iter().map(|record| {
                let table = Table::parse(record);
                table.name
            });

            // join all table names with a space in between
            let table_names = table_names.collect::<Vec<_>>().join(" ");

            println!("{}", table_names);
        }
        _ => bail!("Unrecognized dot command: {}", command),
    }

    Ok(())
}

// TODO: This could be macro'd
trait ByteReader {
    fn read_u8(&mut self) -> u8;
    fn read_u16(&mut self) -> u16;
    fn read_u32(&mut self) -> u32;
    fn read_u64(&mut self) -> u64;
    fn read_i8(&mut self) -> i8;
    fn read_i16(&mut self) -> i16;
    fn read_i32(&mut self) -> i32;
    fn read_i64(&mut self) -> i64;
    fn read_varint(&mut self) -> (u64, usize);
    fn skip(&mut self, n: usize);
}

impl<R: Read> ByteReader for R {
    fn read_u8(&mut self) -> u8 {
        let mut buf = [0; 1];
        self.read_exact(&mut buf).unwrap();
        eprintln!("{:x?}", buf);
        u8::from_be_bytes(buf)
    }

    fn read_u16(&mut self) -> u16 {
        let mut buf = [0; 2];
        self.read_exact(&mut buf).unwrap();
        eprintln!("{:x?}", buf);
        u16::from_be_bytes(buf)
    }

    fn read_u32(&mut self) -> u32 {
        let mut buf = [0; 4];
        self.read_exact(&mut buf).unwrap();
        eprintln!("{:x?}", buf);
        u32::from_be_bytes(buf)
    }

    fn read_u64(&mut self) -> u64 {
        let mut buf = [0; 8];
        self.read_exact(&mut buf).unwrap();
        eprintln!("{:x?}", buf);
        u64::from_be_bytes(buf)
    }

    fn read_i8(&mut self) -> i8 {
        let mut buf = [0; 1];
        self.read_exact(&mut buf).unwrap();
        eprintln!("{:x?}", buf);
        i8::from_be_bytes(buf)
    }

    fn read_i16(&mut self) -> i16 {
        let mut buf = [0; 2];
        self.read_exact(&mut buf).unwrap();
        eprintln!("{:x?}", buf);
        i16::from_be_bytes(buf)
    }

    fn read_i32(&mut self) -> i32 {
        let mut buf = [0; 4];
        self.read_exact(&mut buf).unwrap();
        eprintln!("{:x?}", buf);
        i32::from_be_bytes(buf)
    }

    fn read_i64(&mut self) -> i64 {
        let mut buf = [0; 8];
        self.read_exact(&mut buf).unwrap();
        eprintln!("{:x?}", buf);
        i64::from_be_bytes(buf)
    }

    fn read_varint(&mut self) -> (u64, usize) {
        let mut n = 0;
        let mut shift = 0;
        let mut size = 0;

        loop {
            let mut buf = [0; 1];
            self.read_exact(&mut buf).unwrap();
            size += 1;

            let byte = buf[0] as u64;
            if byte & 0x80 == 0 {
                n <<= shift;
                n |= byte;
                break;
            } else {
                n <<= shift;
                n |= byte & 0x7f;
                shift += 7;
            }
        }

        (n, size)
    }

    fn skip(&mut self, n: usize) {
        let mut buf = vec![0; n];
        self.read_exact(&mut buf).unwrap();
    }
}

#[derive(Debug)]
enum FileFormat {
    Legacy,
    Wal,
}

impl From<u8> for FileFormat {
    fn from(byte: u8) -> Self {
        match byte {
            1 => FileFormat::Legacy,
            2 => FileFormat::Wal,
            _ => panic!("Invalid file format byte: {}", byte),
        }
    }
}

#[derive(Debug)]
enum SchemaFormat {
    One,
    Two,
    Three,
    Four,
}

impl From<u32> for SchemaFormat {
    fn from(n: u32) -> Self {
        match n {
            1 => SchemaFormat::One,
            2 => SchemaFormat::Two,
            3 => SchemaFormat::Three,
            4 => SchemaFormat::Four,
            _ => panic!("Invalid schema format byte: {}", n),
        }
    }
}

#[derive(Debug)]
enum TextEncoding {
    Utf8,
    Utf16le,
    Utf16be,
}

impl From<u32> for TextEncoding {
    fn from(n: u32) -> Self {
        match n {
            1 => TextEncoding::Utf8,
            2 => TextEncoding::Utf16le,
            3 => TextEncoding::Utf16be,
            _ => panic!("Invalid text encoding byte: {}", n),
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct DbHeader {
    page_size: u32,
    file_format_write_version: FileFormat,
    file_format_read_version: FileFormat,
    reserved_space: u8,
    max_embedded_payload_fraction: u8,
    min_embedded_payload_fraction: u8,
    leaf_payload_fraction: u8,
    file_change_counter: u32,
    database_size_in_pages: u32,
    first_freelist_trunk_page: u32,
    number_of_freelist_pages: u32,
    schema_cookie: u32,
    schema_format: SchemaFormat,
    default_page_cache_size: u32,
    largest_root_btree_page_number: u32,
    text_encoding: TextEncoding,
    user_version: u32,
    incremental_vacuum_mode: bool,
    application_id: u32,
    version_valid_for: u32,
    sqlite_version_number: u32,
}

impl DbHeader {
    fn parse<R: Read + ByteReader>(reader: &mut R) -> Self {
        // Every valid SQLite database file begins with the following 16 bytes (in hex):
        // 53 51 4c 69 74 65 20 66 6f 72 6d 61 74 20 33 00.
        // This byte sequence corresponds to the UTF-8 string "SQLite format 3" including the nul
        // terminator character at the end.
        let mut magic = [0; 16];
        reader.read_exact(&mut magic).unwrap();
        assert!(
            magic
                == [
                    0x53, 0x51, 0x4c, 0x69, 0x74, 0x65, 0x20, 0x66, 0x6f, 0x72, 0x6d, 0x61, 0x74,
                    0x20, 0x33, 0x00
                ]
        );

        // The two-byte value beginning at offset 16 determines the page size of the database.
        let page_size = reader.read_u16();

        // The value 65536 will not fit in a two-byte integer, so to specify a 65536-byte page size, the
        // value at offset 16 is 0x00 0x01. This value can be interpreted as a big-endian 1 and thought
        // of as a magic number to represent the 65536 page size.
        let page_size: u32 = if page_size == 1 {
            65536
        } else {
            page_size as u32
        };

        // The file format write version and file format read version at offsets 18 and 19 are intended
        // to allow for enhancements of the file format in future versions of SQLite. In current
        // versions of SQLite, both of these values are 1 for rollback journalling modes and 2 for WAL
        // journalling mode.
        let file_format_write_version = reader.read_u8();
        let file_format_read_version = reader.read_u8();

        // The "reserved space" size in the 1-byte integer at offset 20 is the number of bytes of space
        // at the end of each page to reserve for extensions. This value is usually 0. The value can be odd.
        let reserved_space = reader.read_u8();

        // The maximum and minimum embedded payload fractions and the leaf payload fraction values must
        // be 64, 32, and 32.
        let max_embedded_payload_fraction = reader.read_u8();
        let min_embedded_payload_fraction = reader.read_u8();
        let leaf_payload_fraction = reader.read_u8();

        assert!(max_embedded_payload_fraction == 64);
        assert!(min_embedded_payload_fraction == 32);
        assert!(leaf_payload_fraction == 32);

        // The file change counter is a 4-byte big-endian integer at offset 24 that is incremented
        // whenever the database file is unlocked after having been modified.
        let file_change_counter = reader.read_u32();

        // The 4-byte big-endian integer at offset 28 into the header stores the size of the database
        // file in pages
        // TODO: See specification regarding invalid size with regards to legacy sqlite
        let database_size_in_pages = reader.read_u32();

        // The 4-byte big-endian integer at offset 32 stores the page number of the first page of the
        // freelist, or zero if the freelist is empty. The 4-byte big-endian integer at offset 36 stores
        // the total number of pages on the freelist.
        let first_freelist_trunk_page = reader.read_u32();
        let number_of_freelist_pages = reader.read_u32();

        // The schema cookie is a 4-byte big-endian integer at offset 40 that is incremented whenever
        // the database schema changes
        let schema_cookie = reader.read_u32();

        // The schema format number is a 4-byte big-endian integer at offset 44.
        // The formats are:
        //      1. Format 1 (versions back to 3.0.0)
        //      2. Format 2 (versions 3.1.3 onwards)
        //      3. Format 3 (versions 3.1.4 onwards)
        //      4. Format 4 (versions 3.3.0 onwards)
        let schema_format_number = reader.read_u32();

        // The 4-byte big-endian signed integer at offset 48 is the suggested cache size in pages for
        // the database file.
        let default_page_cache_size = reader.read_u32();

        // If the integer at offset 52 is zero then pointer-map (ptrmap) pages are omitted from the
        // database file and neither auto_vacuum nor incremental_vacuum are supported. If the integer at
        // offset 52 is non-zero then it is the page number of the largest root page in the database file

        let largest_root_btree_page_number = reader.read_u32();

        // The 4-byte big-endian integer at offset 56 determines the encoding used for all text strings
        // stored in the database. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3
        // means UTF-16be. No other values are allowed.
        let text_encoding = reader.read_u32();

        // The 4-byte big-endian integer at offset 60 is the user version which is set and queried by
        // the user_version pragma. The user version is not used by SQLite.
        let user_version = reader.read_u32();

        // the integer at offset 64 is true for incremental_vacuum and false for auto_vacuum. If
        // the integer at offset 52 is zero then the integer at offset 64 must also be zero.
        let incremental_vacuum_mode = reader.read_u32() != 0;
        if largest_root_btree_page_number == 0 {
            assert!(!incremental_vacuum_mode);
        }

        // The 4-byte big-endian integer at offset 68 is an "Application ID" that can be set by the
        // PRAGMA application_id command in order to identify the database as belonging to or associated
        // with a particular application.
        let application_id = reader.read_u32();

        // Skip 20 bytes for the reserved area
        reader.skip(20);

        // The 4-byte big-endian integer at offset 92 is the value of the change counter when the version
        // number was stored. The integer at offset 92 indicates which transaction the version number is
        // valid for and is sometimes called the "version-valid-for number".
        let version_valid_for = reader.read_u32();

        // The 4-byte big-endian integer at offset 96 stores the SQLITE_VERSION_NUMBER value for the
        // SQLite library that most recently modified the database file.
        let sqlite_version_number = reader.read_u32();

        Self {
            page_size,
            file_format_write_version: file_format_write_version.into(),
            file_format_read_version: file_format_read_version.into(),
            reserved_space,
            max_embedded_payload_fraction,
            min_embedded_payload_fraction,
            leaf_payload_fraction,
            file_change_counter,
            database_size_in_pages,
            first_freelist_trunk_page,
            number_of_freelist_pages,
            schema_cookie,
            schema_format: schema_format_number.into(),
            default_page_cache_size,
            largest_root_btree_page_number,
            text_encoding: text_encoding.into(),
            user_version,
            incremental_vacuum_mode,
            application_id,
            version_valid_for,
            sqlite_version_number,
        }
    }
}

#[derive(Debug)]
enum PageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

impl From<u8> for PageType {
    fn from(byte: u8) -> Self {
        match byte {
            0x02 => PageType::InteriorIndex,
            0x05 => PageType::InteriorTable,
            0x0a => PageType::LeafIndex,
            0x0d => PageType::LeafTable,
            _ => panic!("Invalid page type byte: {}", byte),
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct DbPageHeader {
    page_type: PageType,
    first_freeblock: u16,
    cell_count: u16,
    cell_content_area_offset: u16,
    fragmented_free_bytes: u8,
    rightmost_pointer: Option<u32>,
    cells: Vec<u16>,
}

impl DbPageHeader {
    fn parse<R: Read + ByteReader>(reader: &mut R) -> Self {
        // The one-byte flag at offset 0 indicating the b-tree page type.
        //      0x02 interior index b-tree page.
        //      0x05 interior table b-tree page.
        //      0x0a leaf index b-tree page.
        //      0x0d leaf table b-tree page.
        // Any other value for the b-tree page type is an error.
        let flag = reader.read_u8();
        eprintln!("flag: {:x}", flag);
        let page_type = flag.into();

        // The two-byte integer at offset 1 gives the start of the first freeblock on the page, or
        // is zero if there are no freeblocks.
        let first_freeblock = reader.read_u16();
        eprintln!("first_freeblock: {:x}", first_freeblock);

        // The two-byte integer at offset 3 gives the number of cells on the page.
        let cell_count = reader.read_u16();
        eprintln!("cell_count: {:x}", cell_count);

        // The two-byte integer at offset 5 gives the start of the cell content area within the page.
        let cell_content_area_offset = reader.read_u16();
        eprintln!("cell_content_area_offset: {:x}", cell_content_area_offset);

        // The one-byte integer at offset 7 gives the number of fragmented free bytes within the cell
        // content area at the end of the page.
        let fragmented_free_bytes = reader.read_u8();
        eprintln!("fragmented_free_bytes: {:x}", fragmented_free_bytes);

        // The four-byte integer at offset 8 gives the page number of the right-most page in the tree
        // that is the parent of this page. If this is a root page, then the value is zero.
        let rightmost_pointer = match page_type {
            PageType::InteriorIndex | PageType::InteriorTable => Some(reader.read_u32()),
            PageType::LeafIndex | PageType::LeafTable => None,
        };

        // The cell content area consists of a sequence of cells. Each cell has a 2-byte integer
        // giving the size of the cell, followed by the cell content itself. The cell content format
        // depends on the b-tree page type.
        let mut cells = Vec::new();
        for _ in 0..cell_count {
            cells.push(reader.read_u16());
        }

        Self {
            page_type,
            first_freeblock,
            cell_count,
            cell_content_area_offset,
            fragmented_free_bytes,
            rightmost_pointer,
            cells,
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct DbPage<R: Record> {
    header: DbPageHeader,
    records: Vec<R>,
}

impl<R: Record> DbPage<R> {
    fn parse<B: Read + ByteReader + Seek>(reader: &mut B, page_offset: u64) -> Self {
        reader.seek(SeekFrom::Start(page_offset)).unwrap();
        let header = dbg!(DbPageHeader::parse(reader));
        let mut records = vec![];

        eprintln!("header: {:#x?}", header);

        for cell in &header.cells {
            reader
                .seek(SeekFrom::Start(page_offset + *cell as u64))
                .unwrap();
            eprintln!("cell: {:x}", cell);
            let record = R::parse(reader);
            records.push(record);
        }

        Self { header, records }
    }

    fn parse_master<B: Read + ByteReader + Seek>(reader: &mut B) -> Self {
        reader.seek(SeekFrom::Start(100)).unwrap();
        let header = dbg!(DbPageHeader::parse(reader));
        let mut records = vec![];

        eprintln!("header: {:#x?}", header);

        for cell in &header.cells {
            reader.seek(SeekFrom::Start(*cell as u64)).unwrap();
            eprintln!("cell: {:x}", cell);
            let record = R::parse(reader);
            records.push(record);
        }

        Self { header, records }
    }
}

trait Record {
    fn parse<R: Read + ByteReader>(reader: &mut R) -> Self;
}

#[derive(Debug)]
enum DataType {
    Null,
    Int8,
    Int16,
    Int24,
    Int32,
    Int48,
    Int64,
    Float,
    Zero,
    One,
    Blob(usize),
    Text(usize),
}

#[derive(Debug, Clone)]
enum Value {
    Int(i64),
    Text(String),
    Blob(Vec<u8>),
    Null,
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(n) => write!(f, "{}", n),
            Value::Text(s) => write!(f, "{}", s),
            Value::Blob(b) => write!(f, "{:x?}", b),
            Value::Null => write!(f, "NULL"),
        }
    }
}

impl TryInto<i64> for Value {
    type Error = ();

    fn try_into(self) -> Result<i64, Self::Error> {
        match self {
            Value::Int(n) => Ok(n),
            _ => Err(()),
        }
    }
}

impl TryInto<String> for Value {
    type Error = ();

    fn try_into(self) -> Result<String, Self::Error> {
        match self {
            Value::Text(s) => Ok(s),
            Value::Blob(b) => Ok(String::from_utf8(b).unwrap()),
            _ => Err(()),
        }
    }
}

impl TryInto<u32> for Value {
    type Error = ();

    fn try_into(self) -> Result<u32, Self::Error> {
        match self {
            Value::Int(n) => Ok(n as u32),
            _ => Err(()),
        }
    }
}

impl DataType {
    pub fn parse(&self, reader: &mut &[u8]) -> Value {
        match self {
            DataType::Null => Value::Null,
            DataType::Int8 => Value::Int(reader.read_i8() as i64),
            DataType::Int16 => Value::Int(reader.read_i16() as i64),
            DataType::Int24 => {
                let mut buf = [0; 3];
                reader.read_exact(&mut buf).unwrap();
                Value::Int(i32::from_be_bytes([0, buf[0], buf[1], buf[2]]) as i64)
            }
            DataType::Int32 => Value::Int(reader.read_i32() as i64),
            DataType::Int48 => {
                let mut buf = [0; 6];
                reader.read_exact(&mut buf).unwrap();
                Value::Int(i64::from_be_bytes([
                    0, 0, buf[0], buf[1], buf[2], buf[3], buf[4], buf[5],
                ]))
            }
            DataType::Int64 => Value::Int(reader.read_i64()),
            DataType::Float => Value::Int(reader.read_u64() as i64),
            DataType::Zero => Value::Int(0),
            DataType::One => Value::Int(1),
            DataType::Blob(size) => {
                let mut buf = vec![0; *size];
                reader.read_exact(&mut buf).unwrap();
                Value::Blob(buf)
            }
            DataType::Text(size) => {
                let mut buf = vec![0; *size];
                reader.read_exact(&mut buf).unwrap();
                Value::Text(String::from_utf8(buf).unwrap())
            }
        }
    }

    // TODO: account for differing specs of string encoding
    fn parse_string<R: Read + ByteReader>(&self, reader: &mut R) -> String {
        match self {
            DataType::Blob(size) => {
                let mut buf = vec![0; *size];
                reader.read_exact(&mut buf).unwrap();
                String::from_utf8(buf).unwrap()
            }
            DataType::Text(size) => {
                let mut buf = vec![0; *size];
                reader.read_exact(&mut buf).unwrap();
                String::from_utf8(buf).unwrap()
            }
            _ => panic!("Invalid data type for string: {:?}", self),
        }
    }

    fn parse_int<R: Read + ByteReader>(&self, reader: &mut R) -> i64 {
        match self {
            DataType::Int8 => reader.read_i8() as i64,
            DataType::Int16 => reader.read_i16() as i64,
            DataType::Int24 => {
                let mut buf = [0; 3];
                reader.read_exact(&mut buf).unwrap();
                i32::from_be_bytes([0, buf[0], buf[1], buf[2]]) as i64
            }
            DataType::Int32 => reader.read_i32() as i64,
            DataType::Int48 => {
                let mut buf = [0; 6];
                reader.read_exact(&mut buf).unwrap();
                i64::from_be_bytes([0, 0, buf[0], buf[1], buf[2], buf[3], buf[4], buf[5]])
            }
            DataType::Int64 => reader.read_i64(),
            _ => panic!("Invalid data type for int: {:?}", self),
        }
    }
}

impl From<u64> for DataType {
    fn from(byte: u64) -> Self {
        match byte {
            0x00 => DataType::Null,
            0x01 => DataType::Int8,
            0x02 => DataType::Int16,
            0x03 => DataType::Int24,
            0x04 => DataType::Int32,
            0x05 => DataType::Int48,
            0x06 => DataType::Int64,
            0x07 => DataType::Float,
            0x08 => DataType::Zero,
            0x09 => DataType::One,
            byte => {
                if byte >= 12 && byte % 2 == 0 {
                    DataType::Blob(((byte - 12) / 2) as usize)
                } else if byte >= 13 && byte % 2 == 1 {
                    DataType::Text(((byte - 13) / 2) as usize)
                } else {
                    panic!("Invalid data type byte: {}", byte);
                }
            }
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct TableLeafRecord {
    header: TableLeafRecordHeader,
    data_specification: DataSpecification,
    payload: Vec<u8>,
    values: Vec<Value>,
}

#[derive(Debug)]
#[allow(dead_code)]
struct DataSpecification {
    size: usize,
    types: Vec<DataType>,
}

impl DataSpecification {
    fn parse<R: Read + ByteReader>(reader: &mut R, size: usize) -> Self {
        let mut types = vec![];
        let mut payload_reader = vec![0; size];
        reader.read_exact(&mut payload_reader).unwrap();
        let mut payload_reader = payload_reader.as_slice();

        while !payload_reader.is_empty() {
            let (data_type, data_type_size) = payload_reader.read_varint();
            eprintln!("data_type: {:x}", data_type);
            eprintln!("data_type_size: {:x}", data_type_size);
            types.push(data_type.into());
        }

        dbg!(&types);

        Self {
            size: size - 1,
            types,
        }
    }
}

impl Record for TableLeafRecord {
    fn parse<R: Read + ByteReader>(reader: &mut R) -> Self {
        let (size, _) = reader.read_varint();
        eprintln!("size: {:x}", size);
        let (row_id, _) = reader.read_varint();
        eprintln!("row_id: {:x}", row_id);
        let header = TableLeafRecordHeader { size, row_id };
        eprintln!("header: {:#?}", header);
        let mut payload = vec![0; size as usize];
        reader.read_exact(&mut payload).unwrap();

        let mut payload = payload.as_slice();
        let (column_header_size, column_header_size_count) = payload.read_varint();
        eprintln!("column_header_size: {:x}", column_header_size);
        eprintln!("column_header_size_count: {:x}", column_header_size_count);

        let data_specification = DataSpecification::parse(
            &mut payload,
            column_header_size as usize - column_header_size_count,
        );

        let values = data_specification
            .types
            .iter()
            .map(|data_type| data_type.parse(&mut payload))
            .collect();

        Self {
            header,
            data_specification,
            payload: payload.to_vec(),
            values,
        }
    }
}

#[derive(Debug)]
#[allow(dead_code)]
struct TableLeafRecordHeader {
    size: u64,
    row_id: u64,
}

#[derive(Debug)]
#[allow(dead_code)]
struct Table {
    name: String,
    root_page: u32,
    sql: String,
    columns: Vec<String>,
}

impl Table {
    fn parse(record: &TableLeafRecord) -> Self {
        let table_type: String = record.values.get(0).unwrap().clone().try_into().unwrap();
        eprintln!("table_type: {}", table_type);
        assert!(table_type == "table");
        let name: String = record.values.get(1).unwrap().clone().try_into().unwrap();
        eprintln!("name: {}", name);
        let table_name: String = record.values.get(2).unwrap().clone().try_into().unwrap();
        eprintln!("table_name: {}", table_name);
        let root_page: u32 = record.values.get(3).unwrap().clone().try_into().unwrap();
        eprintln!("root_page: {}", root_page);
        let sql: String = record.values.get(4).unwrap().clone().try_into().unwrap();
        eprintln!("sql: {}", sql);

        let columns = Table::analyse_sql_for_column_order(&sql);

        Self {
            name,
            root_page,
            sql,
            columns,
        }
    }

    fn analyse_sql_for_column_order(sql: &str) -> Vec<String> {
        let tokens = lexer::Lexer::new(sql.to_string()).lex();
        let mut parser = parser::Parser::new(tokens);
        let ast = parser.parse_create();

        match ast {
            parser::Ast::CreateTable {
                name: _,
                column_defs: columns,
            } => columns
                .iter()
                .map(|col| match col {
                    parser::Ast::ColumnDef {
                        name,
                        data_type: _,
                        constraints: _,
                    } => name,
                    _ => panic!("Not implemented"),
                })
                .cloned()
                .collect(),
            _ => panic!("failed to parse sql from db file"),
        }
    }

    fn get_column_index(&self, column_name: &str) -> usize {
        self.columns
            .iter()
            .position(|col| col == column_name)
            .unwrap()
    }
}
