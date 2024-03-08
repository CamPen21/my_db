use std::error::Error;
use std::fmt::Display;
use std::fs::OpenOptions;
use std::path::PathBuf;
use std::io::{self, Read, Write};
use std::time::{Duration, SystemTime, UNIX_EPOCH};


const DB_DIR: &'static str = "/Users/camilo/Personal/my_db";

pub struct Entry {
    key: String,
    value: String,
    creation_timestamp: Duration,
}

impl Entry {
    pub fn from(key: &str, value: &str) -> Self {
        Entry {
            key: String::from(key),
            value: String::from(value),
            creation_timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
        }
    }
}
pub struct Segment {
    name: String,
    path: PathBuf,
    created_at: Duration,
}

impl Segment {

    pub fn new() -> Self {
        let name: String = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis().to_string();
        let path = PathBuf::from(DB_DIR).join(format!("{}.segment", &name));
        Segment {
            name,
            path,
            created_at: SystemTime::now().duration_since(UNIX_EPOCH).unwrap(),
        }
    }

    pub fn init(&self) -> Result<usize, io::Error> {
        let file_res = OpenOptions::new().write(true).create(true).open(&self.path);
        match file_res {
            Ok(mut file) => {
                let header = format!("#DATABASE_SEGMENT::{}::{}\n", &self.name, &self.created_at.as_millis());
                file.write(header.as_bytes())
            },
            Err(why) => {
                println!("Hello? {:?}", &self.path);
                Err(why.into())
            }
        }

    }

    pub fn write(&self, entry: &Entry) -> Result<(), Box<dyn Error>> {
        let key = &entry.key;
        let value = &entry.value;
        let mut file = OpenOptions::new().append(true).open(&self.path)?;
        let entry_timestamp = &entry.creation_timestamp.as_millis();
        let raw_entry = format!("{entry_timestamp}:::{key}::{value}\n");
        let result = file.write(raw_entry.as_bytes());
        match result {
            Ok(result) => {
                println!("[DEBUG]: Wrote {} bytes", result);
                Ok(())
            },
            Err(why) => {
                Err(why.into())
            }
        }
    }

    pub fn get_size(&self) -> u64 {
        let file = OpenOptions::new().read(true).open(&self.path);
        file.unwrap().metadata().unwrap().len()
    }
}

#[derive(Debug, Clone)]
struct NoSegmentAvailable;

impl Error for NoSegmentAvailable {}

impl Display for NoSegmentAvailable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "No segment is assigned to this handler")
    }
}

pub struct SegmentHandler {
    size_limit: u64,
    current_segment: Option<Segment>,
}

impl SegmentHandler {
    pub fn new(segments_size: u64) -> Self {
        SegmentHandler {
            size_limit: segments_size,
            current_segment: None,
        }
    }

    pub fn create_segment(&mut self) {
        let new_segment = Segment::new();
        new_segment.init().unwrap();
        self.current_segment = Some(new_segment);
    }

    pub fn find(key: &str) -> Option<String> {
        None
    }

    pub fn add(&mut self, entry: Entry) -> Result<(), Box<dyn Error>> {
        // Initialize a new segment
        if self.current_segment.is_none()
            || self.current_segment.as_ref().unwrap().get_size() >= self.size_limit {
            self.create_segment();
        }
        if let Some(segment) = &self.current_segment {
                match segment.write(&entry) {
                    Ok(_) => Ok(()),
                    Err(why) => Err(why.into())
                }
        } else {
            Err("No current available segment".into())
        }
    }
}


pub struct Database {
    segment_handler: SegmentHandler
}

impl Database {
    pub fn new() -> Self {
        Database{
            segment_handler: SegmentHandler::new(4000)
        }
    }

    // Here probably want to return a more complex structured like a record.
    // Record may have a key(u64), a value and a segment(_id)?
    pub fn create(&mut self, value: String) -> Result<String, Box<dyn Error>> {
        let entry = Entry::from("0", &value);
        match self.segment_handler.add(entry) {
            Ok(_) => Ok("0".to_owned()),
            Err(why) => Err(why.into())
        }
    }

    // Here probably take a key(u64) and a new value.
    // Would taking a record make sense?
    pub fn update(&self, key: &str, value: String) {
        println!("{}:{}", key, value);
    } 

    pub fn delete(&self, key: String) {
        println!("{}", key);
    }

}
