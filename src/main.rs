use db::rusqlite_mod::Rusqlite;
use serde::{Deserialize, Serialize};

//use std::collections::HashSet;
use std::fs::File;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::sync::mpsc::channel;
use std::thread;
//use std::thread::JoinHandle;
use std::time::Duration;
use std::time::Instant;

pub mod db {
    pub mod rusqlite_mod;
}

use db::rusqlite_mod as rdb;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Word {
    word: String,
    pos: String, //part of speech
    lang: String,
    lang_code: String,
    forms: Option<Vec<Form>>,

}
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Form {
    form: String,
    tags: Option<Vec<String>>,
}




//https://kaikki.org/dictionary/rawdata.html
//https://github.com/tatuylonen/wiktextract?tab=readme-ov-file#format-of-the-extracted-word-entries
//https://avi.im/blag/2021/fast-sqlite-inserts/
fn main() -> io::Result<()> {
    
    let dbpath = ""; //path to non-exiting sqlite3 file
    let file_path = ""; //path to the wiktionary dump 
   
    let rdb = rdb::Rusqlite::new(dbpath).unwrap();
   
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);

    let main_start = Instant::now();

    
    // let parse_time = _execute_sync(&mut rdb, reader, 0);
    // println!("db time: {}", Duration::from_nanos(rdb.db_time as u64).as_secs_f64());
    // println!("parse time: {}", Duration::from_nanos(parse_time as u64).as_secs_f64());
    
    _execute_batch(rdb, reader, 0);
    let main_time = main_start.elapsed().as_secs_f64();
    println!("main time: {}", main_time);

    
    
    Ok(())
}

fn _execute_batch(rdb: Rusqlite, reader: BufReader<File>, max_lines: usize) -> () {
    let (s, r) = channel::<Vec<Word>>();

    let mut parse_time: u128= 0;
   
    let mut count = 0;
    let mut word_bulk: Vec<Word> = Vec::new();
    let bulk_size = 5000;

    let db_handle = rdb.receive_entries(r);

    let parse_handle = thread::spawn(move || {
        for line in reader.lines() {
            match line {
                Ok(l) => {
                    let parse_start = Instant::now();
                    count += 1;
                    let w: Word = match serde_json::from_str(&l) {
                        Ok(w) => w,
                        Err(e) => {
                            println!("Error from {}", l);
                            println!("Error: {:?}", e);
                            continue;
                        },
                    };
    
                    parse_time += parse_start.elapsed().as_nanos();
    
                    word_bulk.push(w);
                    if word_bulk.len() >= bulk_size {
                       s.send(word_bulk.clone()).unwrap();
                       word_bulk.clear();
                    }
        
                    
                },
                
                Err(_) => break,
            }
    
            if count >= max_lines && max_lines != 0{
                println!("counted  : {}", count);
                println!("parse time: {}", Duration::from_nanos(parse_time as u64).as_secs_f64());
                break;
            }
        }

        if max_lines == 0 {
            println!("counted  : {}", count);
            println!("parse time: {}", Duration::from_nanos(parse_time as u64).as_secs_f64());
        }
    });
    

    
    
    
    parse_handle.join().unwrap();
    db_handle.join().unwrap();

    ()

}
fn _execute_sync(rdb: &mut Rusqlite, reader: BufReader<File>, max_lines: usize) -> u128 {
    let mut parse_time: u128= 0;
   
    let mut count = 0;
    for line in reader.lines() {
        match line {
            Ok(l) => {
                let parse_start = Instant::now();
                count += 1;
                let w: Word = match serde_json::from_str(&l) {
                    Ok(w) => w,
                    Err(e) => {
                        println!("Error from {}", l);
                        println!("Error: {:?}", e);
                        continue;
                    },
                };

                parse_time += parse_start.elapsed().as_nanos();
                match rdb.insert_entry(&w) {
                    Ok(_flag) => (),//println!("Word {} inserted..", &w.word), 
                    Err(_e) => { 
                        // println!("Error while inserting {}", &w.word);
                        // println!("{:?}", _e);
                        ();
                    },
                };
                
            },
            
            Err(_) => break,
        }

        if count >= max_lines && max_lines != 0{
            break;
        }
    }

    println!("counted  : {}", count);
    parse_time
}
