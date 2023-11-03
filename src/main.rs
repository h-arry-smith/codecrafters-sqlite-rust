use anyhow::{bail, Context, Result};
use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;

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
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}

fn handle_dot_command(command: &str, args: &[String]) -> Result<()> {
    match command {
        "dbinfo" => {
            let path = PathBuf::from(&args[0]);
            let mut file = File::open(path).context("Failed to open database file")?;
            let header = DbHeader::parse(&mut file);
            let first_page = DbPage::parse(&mut file);

            println!("database page size: {}", header.page_size);
            println!("number of tables: {}", first_page.cell_count)
        }
        _ => bail!("Unrecognized dot command: {}", command),
    }

    Ok(())
}

trait ByteReader {
    fn read_u8(&mut self) -> u8;
    fn read_u16(&mut self) -> u16;
    fn read_u32(&mut self) -> u32;
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
struct DbPage {
    page_type: PageType,
    first_freeblock: u16,
    cell_count: u16,
    cell_content_area_offset: u16,
    fragmented_free_bytes: u8,
    rightmost_pointer: Option<u32>,
}

impl DbPage {
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

        Self {
            page_type,
            first_freeblock,
            cell_count,
            cell_content_area_offset,
            fragmented_free_bytes,
            rightmost_pointer,
        }
    }
}
