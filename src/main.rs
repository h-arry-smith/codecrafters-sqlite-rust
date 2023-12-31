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
    master_page: DbPage,
    master_page_records: Vec<MasterPageRecord>,
}

impl Db {
    fn new(path: PathBuf) -> Self {
        let mut file = File::open(path).unwrap();
        let header = DbHeader::parse(&mut file);
        let master_page = DbPage::parse_master(&mut file);

        let master_page_records = master_page
            .records
            .iter()
            .map(MasterPageRecord::parse)
            .collect::<Vec<_>>();

        Self {
            file,
            header,
            master_page,
            master_page_records,
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

    fn get_table(&mut self, table_name: &str) -> &MasterPageRecord {
        self.master_page_records
            .iter()
            .find(|record| {
                record.table_name.to_ascii_lowercase() == table_name.to_ascii_lowercase()
            })
            .unwrap()
    }

    fn get_table_record(&mut self, table_name: &str) -> &TableLeafRecord {
        let table = self
            .master_page
            .records
            .iter()
            .find(|record| {
                let table = MasterPageRecord::parse(record);
                table.name.to_ascii_lowercase() == table_name.to_ascii_lowercase()
            })
            .unwrap();

        match table {
            DbRecord::TableLeafRecord(record) => record,
            _ => panic!("Not implemented"),
        }
    }

    fn load_table(&mut self, table: &MasterPageRecord) -> DbPage {
        let offset = (table.root_page as u64 - 1) * self.header.page_size as u64;
        DbPage::parse(&mut self.file, offset)
    }

    fn load_table_at_page(&mut self, page: u64) -> DbPage {
        let offset = (page - 1) * self.header.page_size as u64;

        DbPage::parse(&mut self.file, offset)
    }

    fn get_table_rows(
        &mut self,
        table: &MasterPageRecord,
        row_ids: &mut Option<Vec<u32>>,
    ) -> Vec<TableLeafRecord> {
        let table_record = self.get_table_record(&table.name);
        let table_key = table_record.header.row_id;
        let db_page = self.load_table(table);

        let mut rows = Vec::new();
        self.recurse_page_for_rows(db_page, table_key, &mut rows, None, row_ids);

        let table_leaf_records = rows
            .iter()
            .map(|row| match row {
                DbRecord::TableLeafRecord(trecord) => trecord.clone(),
                _ => unreachable!(),
            })
            .collect();

        table_leaf_records
    }

    fn recurse_page_for_rows(
        &mut self,
        cur_page: DbPage,
        table_key: u64,
        rows: &mut Vec<DbRecord>,
        where_clause: Option<(usize, &Value)>,
        row_ids: &mut Option<Vec<u32>>,
    ) {
        let look_for_row_ids = row_ids.is_some();

        if look_for_row_ids {}

        if look_for_row_ids && row_ids.as_ref().unwrap().is_empty() {
            return;
        }

        match cur_page.header.page_type {
            PageType::InteriorIndex => {
                for record in cur_page.records.iter() {
                    match record {
                        DbRecord::InteriorIndexRecord(irecord) => {
                            let value = where_clause.unwrap().1;
                            let irecord_value = &irecord.values[0];

                            if irecord_value.as_bytes() > value.as_bytes() {
                                let db_page = self.load_table_at_page(irecord.left_child as u64);
                                self.recurse_page_for_rows(
                                    db_page,
                                    table_key,
                                    rows,
                                    where_clause,
                                    row_ids,
                                );
                                break;
                            } else if irecord_value == value {
                                rows.push((*record).clone());
                                let db_page = self.load_table_at_page(irecord.left_child as u64);
                                self.recurse_page_for_rows(
                                    db_page,
                                    table_key,
                                    rows,
                                    where_clause,
                                    row_ids,
                                );
                                break;
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                let db_page =
                    self.load_table_at_page(cur_page.header.rightmost_pointer.unwrap() as u64);
                self.recurse_page_for_rows(db_page, table_key, rows, where_clause, row_ids);
            }
            PageType::InteriorTable => {
                if look_for_row_ids {
                    let my_row_ids = row_ids.as_mut().unwrap();
                    let first_row_id = my_row_ids.first().unwrap();
                    let first_record = cur_page.records.first().unwrap();
                    let last_record = cur_page.records.last().unwrap();

                    let first_record = match first_record {
                        DbRecord::InteriorTableRecord(irecord) => irecord,
                        _ => unreachable!(),
                    };

                    let last_record = match last_record {
                        DbRecord::InteriorTableRecord(irecord) => irecord,
                        _ => unreachable!(),
                    };

                    if *first_row_id as u64 >= first_record.key {
                        let db_page = self.load_table_at_page(first_record.left_child_page as u64);
                        self.recurse_page_for_rows(db_page, table_key, rows, where_clause, row_ids);
                        return;
                    }

                    if *first_row_id as u64 <= last_record.key {
                        let db_page = self
                            .load_table_at_page(cur_page.header.rightmost_pointer.unwrap() as u64);
                        self.recurse_page_for_rows(db_page, table_key, rows, where_clause, row_ids);
                        return;
                    }
                }

                for record in cur_page.records.iter() {
                    match record {
                        DbRecord::InteriorTableRecord(irecord) => {
                            let db_page = self.load_table_at_page(irecord.left_child_page as u64);
                            self.recurse_page_for_rows(
                                db_page,
                                table_key,
                                rows,
                                where_clause,
                                row_ids,
                            );
                        }
                        _ => unreachable!(),
                    }
                }
            }
            PageType::LeafIndex => {
                for record in cur_page.records.iter() {
                    match record {
                        DbRecord::IndexLeafRecord(ilrecord) => {
                            let value = where_clause.unwrap().1;
                            let ilrecord_value = &ilrecord.values[0];

                            if ilrecord_value == value {
                                rows.push((*record).clone());
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
            PageType::LeafTable => {
                for record in cur_page.records.iter() {
                    match record {
                        DbRecord::TableLeafRecord(trecord) => {
                            if look_for_row_ids {
                                let row_ids = row_ids.as_mut().unwrap();

                                if row_ids.contains(&(trecord.header.row_id as u32)) {
                                    rows.push((*record).clone());
                                    row_ids.retain(|id| id != &(trecord.header.row_id as u32));
                                }
                            } else {
                                rows.push((*record).clone());
                            }
                        }
                        _ => unreachable!(),
                    }
                }
            }
        }
    }

    fn get_index_for_column_and_table(
        &mut self,
        table: &str,
        column_name: &str,
    ) -> Option<MasterPageRecord> {
        self.master_page_records
            .iter()
            .find(|record| {
                record.table_name == table
                    && record.columns.contains(&column_name.to_string())
                    && record.table_type == "index"
            })
            .cloned()
    }

    fn fetch_rows_from_index(
        &mut self,
        index_record: &MasterPageRecord,
        value: &Value,
    ) -> Vec<TableLeafRecord> {
        // FIXME: There aren't just one column in an index
        let column_index = index_record.get_column_index(&index_record.columns[0]);
        let table_key = self
            .get_table_record(&index_record.table_name)
            .header
            .row_id;
        let cur_page = self.load_table_at_page(index_record.root_page as u64);

        let where_clause = Some((column_index, value));

        let mut rows = Vec::new();
        self.recurse_page_for_rows(cur_page, table_key, &mut rows, where_clause, &mut None);

        let row_ids = rows
            .iter()
            .map(|row| match row {
                DbRecord::IndexLeafRecord(ilrecord) => {
                    ilrecord.values[1].clone().try_into().unwrap()
                }
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();

        let table_to_fetch = self.get_table(&index_record.table_name).clone();
        self.get_table_rows(&table_to_fetch, &mut Some(row_ids))
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
            println!("database page size: {}", header.page_size);

            println!("number of tables: {}", master_page.header.cell_count);
        }
        "tables" => {
            println!("number of tables: {}", master_page.header.cell_count);

            let table_names = master_page.records.iter().map(|record| {
                let table = MasterPageRecord::parse(record);
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
        u8::from_be_bytes(buf)
    }

    fn read_u16(&mut self) -> u16 {
        let mut buf = [0; 2];
        self.read_exact(&mut buf).unwrap();
        u16::from_be_bytes(buf)
    }

    fn read_u32(&mut self) -> u32 {
        let mut buf = [0; 4];
        self.read_exact(&mut buf).unwrap();
        u32::from_be_bytes(buf)
    }

    fn read_u64(&mut self) -> u64 {
        let mut buf = [0; 8];
        self.read_exact(&mut buf).unwrap();
        u64::from_be_bytes(buf)
    }

    fn read_i8(&mut self) -> i8 {
        let mut buf = [0; 1];
        self.read_exact(&mut buf).unwrap();
        i8::from_be_bytes(buf)
    }

    fn read_i16(&mut self) -> i16 {
        let mut buf = [0; 2];
        self.read_exact(&mut buf).unwrap();
        i16::from_be_bytes(buf)
    }

    fn read_i32(&mut self) -> i32 {
        let mut buf = [0; 4];
        self.read_exact(&mut buf).unwrap();
        i32::from_be_bytes(buf)
    }

    fn read_i64(&mut self) -> i64 {
        let mut buf = [0; 8];
        self.read_exact(&mut buf).unwrap();
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
        let page_type = flag.into();

        // The two-byte integer at offset 1 gives the start of the first freeblock on the page, or
        // is zero if there are no freeblocks.
        let first_freeblock = reader.read_u16();

        // The two-byte integer at offset 3 gives the number of cells on the page.
        let cell_count = reader.read_u16();

        // The two-byte integer at offset 5 gives the start of the cell content area within the page.
        let cell_content_area_offset = reader.read_u16();

        // The one-byte integer at offset 7 gives the number of fragmented free bytes within the cell
        // content area at the end of the page.
        let fragmented_free_bytes = reader.read_u8();

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
struct DbPage {
    header: DbPageHeader,
    records: Vec<DbRecord>,
}

impl DbPage {
    fn parse<B: Read + ByteReader + Seek>(reader: &mut B, page_offset: u64) -> Self {
        reader.seek(SeekFrom::Start(page_offset)).unwrap();
        let header = DbPageHeader::parse(reader);

        match header.page_type {
            PageType::LeafTable => Self::parse_leaf_table_page(reader, page_offset, header),
            PageType::LeafIndex => Self::parse_leaf_index_page(reader, page_offset, header),
            PageType::InteriorTable => Self::parse_interior_table_page(reader, page_offset, header),
            PageType::InteriorIndex => Self::parse_interior_index_page(reader, page_offset, header),
        }
    }

    fn parse_leaf_table_page<B: Read + ByteReader + Seek>(
        reader: &mut B,
        page_offset: u64,
        header: DbPageHeader,
    ) -> Self {
        let mut records = vec![];

        for cell in &header.cells {
            reader
                .seek(SeekFrom::Start(page_offset + *cell as u64))
                .unwrap();
            let record = DbRecord::parse_table_leaf_record(reader);
            records.push(record);
        }

        Self { header, records }
    }

    fn parse_leaf_index_page<B: Read + ByteReader + Seek>(
        reader: &mut B,
        page_offset: u64,
        header: DbPageHeader,
    ) -> Self {
        let mut records = vec![];

        for cell in &header.cells {
            reader
                .seek(SeekFrom::Start(page_offset + *cell as u64))
                .unwrap();
            let record = DbRecord::parse_index_leaf_record(reader);
            records.push(record);
        }

        Self { header, records }
    }

    fn parse_interior_table_page<B: Read + ByteReader + Seek>(
        reader: &mut B,
        page_offset: u64,
        header: DbPageHeader,
    ) -> Self {
        let mut records = vec![];

        for cell in &header.cells {
            reader
                .seek(SeekFrom::Start(page_offset + *cell as u64))
                .unwrap();
            let record = DbRecord::parse_table_index_record(reader);
            records.push(record);
        }

        Self { header, records }
    }

    fn parse_interior_index_page<B: Read + ByteReader + Seek>(
        reader: &mut B,
        page_offset: u64,
        header: DbPageHeader,
    ) -> Self {
        let mut records = vec![];

        for cell in &header.cells {
            reader
                .seek(SeekFrom::Start(page_offset + *cell as u64))
                .unwrap();
            let record = DbRecord::parse_index_interior_record(reader);
            records.push(record);
        }

        Self { header, records }
    }

    fn parse_master<B: Read + ByteReader + Seek>(reader: &mut B) -> Self {
        reader.seek(SeekFrom::Start(100)).unwrap();
        let header = DbPageHeader::parse(reader);
        let mut records = vec![];

        for cell in &header.cells {
            reader.seek(SeekFrom::Start(*cell as u64)).unwrap();
            let record = DbRecord::parse_table_leaf_record(reader);
            records.push(record);
        }

        Self { header, records }
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::enum_variant_names)]
enum DbRecord {
    TableLeafRecord(TableLeafRecord),
    IndexLeafRecord(IndexLeafRecord),
    InteriorTableRecord(InteriorTableRecord),
    InteriorIndexRecord(InteriorIndexRecord),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct IndexLeafRecord {
    length: u64,
    payload: Vec<u8>,
    oveflow: Option<u32>,
    data_specification: DataSpecification,
    values: Vec<Value>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct InteriorIndexRecord {
    left_child: u32,
    length: u64,
    key: Vec<u8>,
    data_specification: DataSpecification,
    values: Vec<Value>,
}

impl Record for InteriorIndexRecord {
    fn parse<R: Read + ByteReader>(reader: &mut R) -> Self {
        let left_child = reader.read_u32();
        let (length, _) = reader.read_varint();
        let mut key = vec![0; length as usize];
        reader.read_exact(&mut key).unwrap();

        let mut key_reader = key.as_slice();

        let (column_header_size, column_header_size_count) = key_reader.read_varint();

        let data_specification = DataSpecification::parse(
            &mut key_reader,
            column_header_size as usize - column_header_size_count,
        );

        let values = data_specification
            .types
            .iter()
            .map(|data_type| data_type.parse(&mut key_reader))
            .collect();

        Self {
            left_child,
            length,
            key,
            data_specification,
            values,
        }
    }
}

impl Record for IndexLeafRecord {
    fn parse<R: Read + ByteReader>(reader: &mut R) -> Self {
        let (length, _) = reader.read_varint();
        let mut payload: Vec<u8> = vec![0; length as usize];
        reader.read_exact(&mut payload).unwrap();

        let mut key_reader = payload.as_slice();

        let (column_header_size, column_header_size_count) = key_reader.read_varint();

        let data_specification = DataSpecification::parse(
            &mut key_reader,
            column_header_size as usize - column_header_size_count,
        );

        let values = data_specification
            .types
            .iter()
            .map(|data_type| data_type.parse(&mut key_reader))
            .collect();

        Self {
            length,
            payload,
            oveflow: None,
            data_specification,
            values,
        }
    }
}

impl DbRecord {
    fn parse_table_leaf_record<R: Read + ByteReader>(reader: &mut R) -> Self {
        let record = TableLeafRecord::parse(reader);
        Self::TableLeafRecord(record)
    }

    fn parse_index_leaf_record<R: Read + ByteReader>(reader: &mut R) -> Self {
        let record = IndexLeafRecord::parse(reader);
        Self::IndexLeafRecord(record)
    }

    fn parse_table_index_record<R: Read + ByteReader>(reader: &mut R) -> Self {
        let record = InteriorTableRecord::parse(reader);
        Self::InteriorTableRecord(record)
    }

    fn parse_index_interior_record<R: Read + ByteReader>(reader: &mut R) -> Self {
        let record = InteriorIndexRecord::parse(reader);
        Self::InteriorIndexRecord(record)
    }
}

trait Record {
    fn parse<R: Read + ByteReader>(reader: &mut R) -> Self;
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum Value {
    Int(i64),
    Text(String),
    Blob(Vec<u8>),
    Null,
}

impl Value {
    fn as_bytes(&self) -> Vec<u8> {
        match self {
            Value::Int(n) => n.to_be_bytes().to_vec(),
            Value::Text(s) => s.as_bytes().to_vec(),
            Value::Blob(b) => b.clone(),
            Value::Null => vec![],
        }
    }
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

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct TableLeafRecord {
    header: TableLeafRecordHeader,
    data_specification: DataSpecification,
    payload: Vec<u8>,
    values: Vec<Value>,
}

#[derive(Debug, Clone)]
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
            let (data_type, _) = payload_reader.read_varint();
            types.push(data_type.into());
        }

        Self {
            size: size - 1,
            types,
        }
    }
}

impl Record for TableLeafRecord {
    fn parse<R: Read + ByteReader>(reader: &mut R) -> Self {
        let (size, _) = reader.read_varint();
        let (row_id, _) = reader.read_varint();
        let header = TableLeafRecordHeader { size, row_id };
        let mut payload = vec![0; size as usize];
        reader.read_exact(&mut payload).unwrap();

        let mut payload = payload.as_slice();
        let (column_header_size, column_header_size_count) = payload.read_varint();

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

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct TableLeafRecordHeader {
    size: u64,
    row_id: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct InteriorTableRecord {
    left_child_page: u32,
    key: u64,
}

impl Record for InteriorTableRecord {
    fn parse<R: Read + ByteReader>(reader: &mut R) -> Self {
        let left_child_page = reader.read_u32();
        let key = reader.read_varint().0;

        Self {
            left_child_page,
            key,
        }
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct MasterPageRecord {
    table_type: String,
    name: String,
    table_name: String,
    root_page: u32,
    sql: String,
    columns: Vec<String>,
}

impl MasterPageRecord {
    fn parse(record: &DbRecord) -> Self {
        let record = match record {
            DbRecord::TableLeafRecord(record) => record,
            _ => panic!("Not implemented"),
        };

        let table_type: String = record.values.get(0).unwrap().clone().try_into().unwrap();
        let name: String = record.values.get(1).unwrap().clone().try_into().unwrap();
        let table_name: String = record.values.get(2).unwrap().clone().try_into().unwrap();
        let root_page: u32 = record.values.get(3).unwrap().clone().try_into().unwrap();
        let sql: String = record.values.get(4).unwrap().clone().try_into().unwrap();

        let columns = MasterPageRecord::analyse_sql_for_column_order(&sql);

        Self {
            table_type,
            name,
            table_name,
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
            parser::Ast::CreateIndex {
                name: _,
                table_name: _,
                columns,
            } => {
                let mut columns = columns
                    .iter()
                    .map(|col| match col {
                        parser::Ast::Identifier(name) => name,
                        _ => panic!("Not implemented"),
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                columns.sort();
                columns
            }
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
