use std::path::Path;
use std::sync::mpsc::Receiver;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use std::{collections::HashMap, fmt::Display};

use rusqlite::Connection;
use rusqlite;

use crate::Word;

#[derive(Debug)]
pub enum DbError {
    RusqliteError(rusqlite::Error),
    RusqliteError2 { inner: rusqlite::Error, query: String },

}

// pub struct RusqliteError2 {
//     inner: rusqlite::Error,
//     query: String,
// }
impl Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RusqliteError(e) => 
                write!(f, "{}", e),
            Self::RusqliteError2 { inner, query } => 
                write!(f, "iner: {}, query: {}", inner, query),
        }
    }
}

impl From<rusqlite::Error> for DbError {
    fn from(err: rusqlite::Error) -> Self {
        DbError::RusqliteError(err)
    }
}


pub struct Rusqlite {
    pub conn: Connection,
    langs: HashMap<String, i64>,
    speech_parts: HashMap<String, i64>,
    pub db_time: u128,

}

impl Rusqlite {
    pub fn new<P: AsRef<Path>>(_path: P) -> Result<Self, DbError> {
        //let conn = Connection::open_in_memory()?;
        let conn = Connection::open(_path)?;

        let rusqlite = Rusqlite {
            conn,
            langs: HashMap::new(),
            speech_parts: HashMap::new(),
            db_time: 0,
        };

        rusqlite.create_tables()?;
        
        Ok(rusqlite)
    }

    pub fn receive_entries(mut self, r: Receiver<Vec<Word>>) -> JoinHandle<()> {
        
        let handle = thread::spawn(move || loop {
            match r.recv() {
                Ok(word_package) => {
                    self.bulk_insert(&word_package).unwrap();
                    
                },
                Err(_e) => {
                    println!("db time: {}", Duration::from_nanos(self.db_time as u64).as_secs_f64());
                    println!("error: {:?}", _e);
                    break;
                },
                
            }
        });

        handle
    }

    pub fn bulk_insert(&mut self, package: &Vec<Word>) -> Result<(), DbError> {
        
        let start = Instant::now();

        for w in package {
            if !self.langs.contains_key(&w.lang) {
                self.conn.execute(
                    "INSERT INTO languages (label, lang_code) VALUES (?1, ?2)", 
                    (&w.lang, &w.lang_code))?;
                let lang_id = self.conn.last_insert_rowid();
                self.langs.insert(w.lang.to_string(), lang_id);
            }
    
            if !self.speech_parts.contains_key(&w.pos) {
                self.conn.execute(
                    "INSERT INTO part_of_speech (label) VALUES (?1)", 
                    [&w.pos])?;
                let pos_id = self.conn.last_insert_rowid();
                self.speech_parts.insert(w.pos.to_string(), pos_id);
            }
        }

        let tx = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare_cached("INSERT INTO words (word, pos, lang) VALUES (?1, ?2, ?3)")?;
            let mut stmt2 = tx.prepare_cached("INSERT INTO word_forms (word_form, tag) VALUES (?1, ?2)")?;

            for w in package {
                let pos_id = self.speech_parts.get(&w.pos).unwrap_or(&0);
                let lang_id = self.langs.get(&w.lang).unwrap_or(&0);
                match stmt.execute((&w.word, pos_id, lang_id)) {
                    Ok(_) => (),
                    Err(_e) => {
                        // println!("Error bulk insert: {:?}", e);
                        // println!("Stmt:{:?}", stmt.expanded_sql().unwrap());
                    },
                }

                if let Some(forms) = &w.forms {
                    for f in forms { 
                        if let Some(tags) = &f.tags {
                            for tag in tags {
                                stmt2.execute((&f.form, tag))?;
                            }    
                        }
                      
                   }
                }
            }
        }
        let r = tx.commit().map_err(|e| DbError::RusqliteError(e));
        self.db_time += start.elapsed().as_nanos();
        match r {
            Ok(_) => Ok(()),
            Err(e) => Err(e),
        }
    }
    pub fn insert_entry(&mut self, w: &Word) -> Result<bool, DbError> {

        let start = Instant::now();
        let mut lang_id = 0;
        let mut pos_id = 0;
        if !self.langs.contains_key(&w.lang) {
            self.conn.execute(
                "INSERT INTO languages (label, lang_code) VALUES (?1, ?2)", 
                (&w.lang, &w.lang_code))?;
            lang_id = self.conn.last_insert_rowid();
            self.langs.insert(w.lang.to_string(), lang_id);
        } else {
            if let Some((_, lang_value)) = self.langs.get_key_value(&w.lang) {
                lang_id = lang_value.to_owned();
            }
        }

        if !self.speech_parts.contains_key(&w.pos) {
            self.conn.execute(
                "INSERT INTO part_of_speech (label) VALUES (?1)", 
                [&w.pos])?;
            pos_id = self.conn.last_insert_rowid();
            self.speech_parts.insert(w.pos.to_string(), pos_id);
        } else {
            if let Some((_, pos_value)) = self.speech_parts.get_key_value(&w.pos) {
                pos_id = pos_value.to_owned();
            }
        }

        let mut word_stmt = self.conn.prepare_cached("INSERT INTO words (word, pos, lang) VALUES (?1, ?2, ?3)")?;
        word_stmt.execute((&w.word, pos_id, lang_id)).map_err(|e| DbError::RusqliteError2 { inner: e, query:  word_stmt.expanded_sql().unwrap()})?;
        
        if let Some(forms) = &w.forms {
            for f in forms { 
                if let Some(tags) = &f.tags {
                    for tag in tags {
                        self.conn.execute("INSERT INTO word_forms (word_form, tag) VALUES (?1, ?2)", 
                        (&f.form, tag))?;
                    }    
                }
              
           }
        }

        self.db_time += start.elapsed().as_nanos();
        Ok(true)
    }
    fn create_tables(&self) -> Result<bool, DbError> {

        self.conn.execute_batch(
            "PRAGMA journal_mode = OFF;
                  PRAGMA synchronous = 0;
                  PRAGMA cache_size = 1000000;
                  PRAGMA locking_mode = EXCLUSIVE;
                  PRAGMA temp_store = MEMORY;",
        )
        .expect("PRAGMA");

        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS part_of_speech (
                pos_id INTEGER PRIMARY KEY,
                label TEXT NOT NULL,
                UNIQUE (label)
            )", 
            ()
        )?;
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS languages (
                lang_id INTEGER PRIMARY KEY,
                label TEXT NOT NULL,
                lang_code TEXT NOT NULL,
                UNIQUE (label, lang_code)
            )", 
            ()
        )?;
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS word_forms (
                form_id INTEGER PRIMARY KEY,
                word_form TEXT NOT NULL,
                tag TEXT NOT NULL
            )", 
            ()
        )?;
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS words (
                word_id INTEGER PRIMARY KEY,
                word TEXT NOT NULL COLLATE NOCASE,
                pos INTEGER NOT NULL,
                lang INTEGER NOT NULL,
                FOREIGN KEY (pos) REFERENCES part_of_speech (pos_id),
                FOREIGN KEY (lang) REFERENCES languages (lang_id),
                UNIQUE (word, pos)
            )", 
            ()
        )?;

        self.conn.execute(
            "CREATE INDEX word_text_idx on words (word COLLATE NOCASE)", 
            ()
        )?;
        Ok(true)
    }

    fn _insert_dummy_data(&self) -> Result<bool, DbError> {

        let types = ["noun", "adverb", "adjective"];
        let words = ["one", "two", "three"];

        for (i, t) in types.into_iter().enumerate() {
            self.conn.execute(
                "INSERT INTO word_types (type_name) VALUES (?1)", 
               [t]
            )?;
            let last = self.conn.last_insert_rowid();

            self.conn.execute(
                "INSERT INTO word (word, word_type) VALUES (?1, ?2)", 
                (words[i], last)
            )?;
        }

       
        Ok(true)
    }

    fn _dump_all(&self) -> Result<bool, DbError> {
        let mut stmt = self.conn.prepare("SELECT * FROM word")?;
        let mut rows = stmt.query([])?;

        while let Some(row) = rows.next()? {
            println!("{:?}", row);
        }
        Ok(true)
    }
}


