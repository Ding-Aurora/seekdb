// Copyright (c) 2026 OceanBase.
// Licensed under the Apache License, Version 2.0.

//! Bounded, read-only file decoders. SQL planning and evaluation stay in C++.
use crate::bridge::ffi::{FileCell, FileColumn};
use chrono::{DateTime, NaiveDate, NaiveDateTime};
use parquet::{basic::{ConvertedType, Type as PhysicalType}, file::{reader::FileReader, serialized_reader::SerializedFileReader}, record::{Field, reader::RowIter}, schema::types::Type};
use serde::{Deserialize, Serialize};
use std::{collections::HashSet, fs::{File, Metadata}, io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write}, path::{Path, PathBuf}, sync::atomic::{AtomicU64, Ordering}};

const MAX_ROW: usize = 16 * 1024 * 1024;
const MAX_COLUMNS: usize = 1024;
const MAX_FILES: usize = 10000;
const END: i32 = 100;
#[derive(Debug)]
struct Error(i32, String);
type Result<T> = std::result::Result<T, Error>;
fn invalid(message: impl Into<String>) -> Error { Error(-1, message.into()) }
fn unsupported(message: impl Into<String>) -> Error { Error(-2, message.into()) }
fn io(error: impl std::fmt::Display) -> Error { Error(-3, error.to_string()) }

// Kind numbers are stable across the CXX boundary and the serialized plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum Kind { Null=0, Integer=1, Double=2, Boolean=3, Text=4, Date=5, DateTime=6 }
#[derive(Clone, Serialize, Deserialize)]
struct Column { name: String, kind: Kind, nullable: bool, #[serde(default)] scale:u8 }
#[derive(Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
struct Fingerprint { size: u64, modified_ns: u128, device: u64, inode: u64 }
impl Fingerprint {
    fn from(meta: &Metadata) -> Result<Self> {
        #[cfg(unix)]
        let (device,inode) = { use std::os::unix::fs::MetadataExt; (meta.dev(),meta.ino()) };
        #[cfg(not(unix))]
        let (device,inode) = (0,0);
        Ok(Self {size:meta.len(), modified_ns:meta.modified().map_err(io)?.duration_since(std::time::UNIX_EPOCH).map_err(io)?.as_nanos(),device,inode})
    }
}
#[derive(Clone, Serialize, Deserialize)]
struct Snapshot { path: String, format: String, options: serde_json::Value, columns: Vec<Column>, fingerprint: Fingerprint }
#[derive(Clone)]
struct Cell { text: String, kind: Kind }
impl Cell { fn null() -> Self { Self {text:String::new(),kind:Kind::Null} } }

fn canonical_allowed(path: &Path, root: &str) -> Result<PathBuf> {
    if root.is_empty() || root.eq_ignore_ascii_case("NULL") { return Err(Error(-4,"secure_file_priv disables file access".into())); }
    let allowed=std::fs::canonicalize(root).map_err(io)?;
    let actual=std::fs::canonicalize(path).map_err(io)?;
    if !actual.starts_with(&allowed) { return Err(Error(-4,"path is outside secure_file_priv".into())); }
    Ok(actual)
}
fn checked_open(path: &str, root: &str) -> Result<(File,PathBuf)> {
    let actual=canonical_allowed(Path::new(path),root)?;
    let file=File::open(&actual).map_err(io)?;
    if !file.metadata().map_err(io)?.is_file() { return Err(invalid("input is not a regular file")); }
    #[cfg(target_os="linux")]
    { use std::os::fd::AsRawFd;
      let opened=std::fs::read_link(format!("/proc/self/fd/{}",file.as_raw_fd())).map_err(io)?;
      canonical_allowed(&opened,root)?;
      if opened != actual { return Err(Error(-5,"file path changed while opening".into())); }
    }
    Ok((file,actual))
}

struct CsvField { text:String, quoted:bool }
struct CsvReader { input:BufReader<File>, delimiter:u8, quote:u8, escape:u8 }
impl CsvReader {
    fn byte(&mut self) -> Result<Option<u8>> {
        let buffer=self.input.fill_buf().map_err(io)?;
        let value=buffer.first().copied();
        if value.is_some() {self.input.consume(1);}
        Ok(value)
    }
    fn record(&mut self) -> Result<Option<Vec<CsvField>>> {
        let mut fields=Vec::new();let mut field=Vec::new();let mut quoted=false;
        let mut state=0;let mut bytes=0;
        loop {
            let next=self.byte()?;
            if next.is_none() && bytes==0 {return Ok(None);}
            bytes+=1;
            if bytes>MAX_ROW {return Err(Error(-6,"CSV record exceeds 16 MiB".into()));}
            let end=next.is_none();let ch=next.unwrap_or(b'\n');
            if end && (state==2 || state==4) {return Err(invalid("unterminated quoted CSV field"));}
            if state==4 {field.push(ch);state=2;continue;}
            if state==2 {
                if ch==self.quote {state=3;}
                else if ch==self.escape && self.escape!=self.quote {state=4;}
                else {field.push(ch);}
                continue;
            }
            if state==3 && ch==self.quote {field.push(ch);state=2;continue;}
            if ch==self.delimiter || ch==b'\n' || ch==b'\r' || end {
                if fields.len()>=MAX_COLUMNS {return Err(Error(-6,"more than 1024 CSV fields".into()));}
                fields.push(CsvField{text:String::from_utf8(std::mem::take(&mut field)).map_err(io)?,quoted});
                if ch==self.delimiter && !end {state=0;quoted=false;continue;}
                if ch==b'\r' && self.input.fill_buf().map_err(io)?.first()==Some(&b'\n') {self.input.consume(1);}
                return Ok(Some(fields));
            }
            if state==3 {return Err(invalid("unexpected character after closing CSV quote"));}
            if ch==self.quote {
                if state!=0 {return Err(invalid("quote inside unquoted CSV field"));}
                quoted=true;state=2;
            } else {field.push(ch);state=1;}
        }
    }
}
enum Backend {
    Csv { reader: CsvReader, names:Vec<String>, nulls:Vec<String> },
    Json(BufReader<File>),
    Parquet(RowIter<'static>),
    Glob(std::vec::IntoIter<PathBuf>),
}
pub struct FileSource {
    snapshot: Option<Snapshot>, backend: Option<Backend>, root:String,
    fingerprint_file: Option<File>, row:Vec<FileCell>, projection:Vec<i32>,
    position:u64, status:i32, error:String,
}
impl FileSource {
    fn empty(root:&str) -> Self { Self {snapshot:None,backend:None,root:root.into(),fingerprint_file:None,row:Vec::new(),projection:Vec::new(),position:0,status:0,error:String::new()} }
    fn fail(&mut self, error:Error) { self.status=error.0; self.error=format!("{}: record {}: {}",self.snapshot.as_ref().map_or("file",|s|s.path.as_str()),self.position+1,error.1);self.backend=None;self.fingerprint_file=None; }
    pub fn file_status(&self) -> i32 { self.status }
    pub fn file_error(&self) -> String { self.error.clone() }
    pub fn file_snapshot(&self) -> String { self.snapshot.as_ref().and_then(|s|serde_json::to_string(s).ok()).unwrap_or_default() }
    pub fn file_columns(&self) -> Vec<FileColumn> {
        self.snapshot.as_ref().map_or_else(Vec::new,|s| {
            if describe(s) { ["column_name","column_type","null","key","default","extra"].iter().enumerate().map(|(index,name)|FileColumn{name:(*name).into(),kind:Kind::Text as u8,nullable:index>=3}).collect() }
            else {s.columns.iter().map(|c|FileColumn {name:c.name.clone(),kind:(c.kind as u8) | (c.scale << 4),nullable:c.nullable}).collect()}
        })
    }
    pub fn file_row(&self) -> &[FileCell] { &self.row }
    fn unchanged(&self) -> Result<()> {
        if let (Some(file),Some(snapshot))=(&self.fingerprint_file,&self.snapshot) {
            if Fingerprint::from(&file.metadata().map_err(io)?)? != snapshot.fingerprint { return Err(Error(-5,"file changed after schema inference".into())); }
            let path_meta=std::fs::metadata(&snapshot.path).map_err(io)?;
            if Fingerprint::from(&path_meta)? != snapshot.fingerprint { return Err(Error(-5,"file was replaced after schema inference".into())); }
        }
        Ok(())
    }
    fn raw_next(&mut self) -> Result<Option<Vec<(String,Cell)>>> {
        self.unchanged()?;
        let result=match self.backend.as_mut().ok_or_else(||invalid("reader is closed"))? {
            Backend::Csv {reader,names,nulls} => {
                if let Some(record)=reader.record()? {
                    if record.len()!=names.len() { return Err(invalid(format!("CSV has {} fields, expected {}",record.len(),names.len()))); }
                    Some(names.iter().cloned().zip(record.iter().map(|field|if !field.quoted && nulls.contains(&field.text) {Cell::null()} else {text_cell(&field.text)})).collect())
                } else {None}
            },
            Backend::Json(reader) => {
                let mut line=Vec::new();
                loop {
                    let buffer=reader.fill_buf().map_err(io)?;
                    if buffer.is_empty() {break;}
                    let n=buffer.iter().position(|&b|b==b'\n').map_or(buffer.len(),|n|n+1);
                    if line.len()+n>MAX_ROW {return Err(Error(-6,"JSONL record exceeds 16 MiB".into()));}
                    line.extend_from_slice(&buffer[..n]);let end=buffer[n-1]==b'\n';reader.consume(n);if end {break;}
                }
                if line.is_empty() {None} else {
                    let value:serde_json::Value=serde_json::from_slice(&line).map_err(io)?;
                    let object=value.as_object().ok_or_else(||invalid("JSONL requires a flat object per line"))?;
                    if object.len()>MAX_COLUMNS {return Err(Error(-6,"too many JSON fields".into()));}
                    let mut fields=Vec::new();
                    for (name,value) in object {fields.push((name.clone(),json_cell(value)?));}
                    Some(fields)
                }
            },
            Backend::Parquet(rows) => match rows.next() {
                None=>None,
                Some(row)=> {let row=row.map_err(io)?;let mut fields=Vec::new();for (name,value) in row.get_column_iter() {fields.push((name.clone(),parquet_cell(value)?));}Some(fields)},
            },
            Backend::Glob(paths) => paths.next().map(|path|vec![("file".into(),Cell{text:path.to_string_lossy().into_owned(),kind:Kind::Text})]),
        };
        self.unchanged()?;
        if result.is_some() {self.position+=1;}
        Ok(result)
    }
    pub fn file_next(&mut self) -> i32 {
        self.row.clear();
        if self.status<0 {return self.status;}
        if self.snapshot.as_ref().is_some_and(describe) {
            if let Err(error)=self.unchanged() {self.fail(error);return self.status;}
            let snapshot=self.snapshot.as_ref().expect("snapshot checked above");
            if self.position as usize>=snapshot.columns.len() {self.status=END;return END;}
            let column=&snapshot.columns[self.position as usize];
            let names=["NULL","BIGINT","DOUBLE","BOOLEAN","VARCHAR","DATE","DATETIME"];
            let cells=[Some(column.name.clone()),Some(names[column.kind as usize].into()),
                Some(if column.nullable {"YES"} else {"NO"}.into()),None,None,None];
            let indices:Vec<usize>=if self.projection.is_empty() {(0..6).collect()} else {self.projection.iter().map(|&i|i as usize).collect()};
            self.row=indices.into_iter().map(|i|FileCell{text:cells[i].clone().unwrap_or_default(),is_null:cells[i].is_none()}).collect();
            self.position+=1;self.status=0;return 0;
        }
        match self.raw_next() {
            Ok(None)=>self.status=END,
            Ok(Some(raw))=> {
                let snapshot=self.snapshot.as_ref().expect("opened source has a snapshot");
                let selected:Vec<usize>=if self.projection.is_empty() {(0..snapshot.columns.len()).collect()} else {self.projection.iter().map(|&i|i as usize).collect()};
                for index in selected {
                    let column=&snapshot.columns[index];
                    let value=raw.iter().find(|(name,_)|name==&column.name).map(|(_,value)|value);
                    match value {
                        None=>self.row.push(FileCell{text:String::new(),is_null:true}),
                        Some(value)=>self.row.push(FileCell{text:if column.kind==Kind::Boolean && value.kind!=Kind::Null {if value.text.eq_ignore_ascii_case("true") {"1".into()} else {"0".into()}} else {value.text.clone()},is_null:value.kind==Kind::Null}),
                    }
                }
                self.status=0;
            },
            Err(error)=>self.fail(error),
        }
        self.status
    }
    pub fn file_rescan(&mut self) -> i32 {
        let result=self.unchanged().and_then(|_|self.open_backend());
        if let Err(error)=result {self.fail(error);} else {self.position=0;self.status=0;self.row.clear();}
        self.status
    }
    fn open_backend(&mut self) -> Result<()> {
        let snapshot=self.snapshot.as_ref().ok_or_else(||invalid("missing file snapshot"))?;
        if snapshot.format=="glob" {
            self.backend=Some(Backend::Glob(discover(&snapshot.path,&self.root)?.into_iter()));return Ok(());
        }
        let (file,_)=checked_open(&snapshot.path,&self.root)?;
        if Fingerprint::from(&file.metadata().map_err(io)?)? != snapshot.fingerprint {return Err(Error(-5,"file changed before execution".into()));}
        self.fingerprint_file=Some(file.try_clone().map_err(io)?);
        let options=&snapshot.options;
        self.backend=Some(match snapshot.format.as_str() {
            "csv"=> {
                let delimiter=option_byte(options,"delim",b',')?;
                let quote=option_byte(options,"quote",b'"')?;
                let escape=option_byte(options,"escape",quote)?;
                if delimiter==quote || delimiter==escape {return Err(invalid("CSV delimiter must differ from quote and escape"));}
                let mut reader=CsvReader{input:BufReader::new(file),delimiter,quote,escape};
                let header=options.get("header").and_then(|v|v.as_bool()).unwrap_or(true);
                let names=if header {
                    unique_names(reader.record()?.unwrap_or_default().into_iter().map(|field|field.text).collect())?
                } else if snapshot.columns.is_empty() {
                    let first=reader.record()?.unwrap_or_default();
                    let names=(0..first.len()).map(|n|format!("column{}",n+1)).collect();
                    reader.input.seek(SeekFrom::Start(0)).map_err(io)?;names
                } else {snapshot.columns.iter().map(|c|c.name.clone()).collect()};
                let nulls=match options.get("nullstr") {Some(serde_json::Value::String(s))=>vec![s.clone()],Some(serde_json::Value::Array(a))=>a.iter().map(|v|v.as_str().map(str::to_owned).ok_or_else(||invalid("nullstr array must contain strings"))).collect::<Result<Vec<_>>>()?,None=>vec![String::new()],_=>return Err(invalid("invalid nullstr"))};
                Backend::Csv {reader,names,nulls}
            },
            "json"=>Backend::Json(BufReader::new(file)),
            "parquet"=> {
                let reader=parquet_reader(file)?;
                let projection=if self.projection.is_empty() || describe(snapshot) {None} else {
                    let fields=reader.metadata().file_metadata().schema().get_fields();
                    let selected=self.projection.iter().map(|&index|fields[index as usize].clone()).collect();
                    Some(Type::group_type_builder(reader.metadata().file_metadata().schema().name()).with_fields(selected).build().map_err(io)?)
                };
                Backend::Parquet(RowIter::from_file_into(Box::new(reader)).project(projection).map_err(io)?)
            },
            _=>return Err(unsupported("unsupported file format")),
        });
        Ok(())
    }
}

fn option_byte(options:&serde_json::Value,name:&str,default:u8) -> Result<u8> {
    match options.get(name) {None=>Ok(default),Some(v)=> {let text=v.as_str().ok_or_else(||invalid(format!("{name} requires one character")))?;if text.len()==1 && text.as_bytes()[0]!=b'\n' && text.as_bytes()[0]!=b'\r' {Ok(text.as_bytes()[0])} else {Err(invalid(format!("{name} requires one byte")))}}}
}
fn describe(snapshot:&Snapshot) -> bool {snapshot.options.get("__describe").and_then(|v|v.as_bool()).unwrap_or(false)}
fn unique_names(input:Vec<String>) -> Result<Vec<String>> {
    if input.len()>MAX_COLUMNS {return Err(Error(-6,"more than 1024 file columns".into()));}
    let mut used=HashSet::new();let mut output=Vec::new();
    for (index,original) in input.into_iter().enumerate() {
        let name=if original.is_empty() {format!("column{}",index+1)} else {original};
        let mut candidate=name.clone();let mut suffix=2;
        while !used.insert(candidate.to_lowercase()) {candidate=format!("{name}__{suffix}");suffix+=1;}
        output.push(candidate);
    }
    Ok(output)
}
fn datetime_scale(cell:&Cell) -> u8 {
    if cell.kind==Kind::DateTime {cell.text.get(19..).and_then(|tail|tail.strip_prefix('.')).map_or(0,|fraction|fraction.len().min(6) as u8)} else {0}
}
fn text_cell(text:&str) -> Cell {
    let kind=if text.eq_ignore_ascii_case("true") || text.eq_ignore_ascii_case("false") {Kind::Boolean}
        else if text.parse::<i64>().is_ok() {Kind::Integer}
        else if text.len()==10 && NaiveDate::parse_from_str(text,"%Y-%m-%d").is_ok() {Kind::Date}
        else if text.len()>=19 && NaiveDateTime::parse_from_str(&text.replace('T'," "),"%Y-%m-%d %H:%M:%S%.f").is_ok() {Kind::DateTime}
        else if (text.contains('.') || text.contains('e') || text.contains('E')) && text.parse::<f64>().is_ok_and(f64::is_finite) {Kind::Double}
        else {Kind::Text};
    Cell{text:text.into(),kind}
}
fn json_cell(value:&serde_json::Value) -> Result<Cell> {
    Ok(match value {
        serde_json::Value::Null=>Cell::null(),
        serde_json::Value::Bool(v)=>Cell{text:v.to_string(),kind:Kind::Boolean},
        serde_json::Value::Number(v)=>Cell{text:v.to_string(),kind:if v.is_i64() {Kind::Integer} else if v.is_u64() {Kind::Text} else {Kind::Double}},
        serde_json::Value::String(v)=> {
            let mut value=text_cell(v);
            if !matches!(value.kind,Kind::Date|Kind::DateTime) {value.kind=Kind::Text;}
            value
        },
        _=>return Err(unsupported("nested JSON objects and arrays are not supported")),
    })
}
fn merge(left:Kind,right:Kind) -> Kind {
    if left==Kind::Null {right} else if right==Kind::Null || left==right {left}
    else if matches!((left,right),(Kind::Integer,Kind::Double)|(Kind::Double,Kind::Integer)) {Kind::Double}
    else if matches!((left,right),(Kind::Date,Kind::DateTime)|(Kind::DateTime,Kind::Date)) {Kind::DateTime}
    else {Kind::Text}
}
fn discover(pattern:&str,root:&str) -> Result<Vec<PathBuf>> {
    let prefix=pattern.find(['*','?','[']).unwrap_or(pattern.len());
    let literal=&pattern[..prefix];
    let parent=if literal.ends_with(std::path::MAIN_SEPARATOR) {Path::new(literal)}
        else {Path::new(literal).parent().unwrap_or(Path::new("."))};
    canonical_allowed(parent,root)?;
    let mut files=Vec::new();
    for item in glob::glob(pattern).map_err(io)? {
        let path=item.map_err(io)?;
        let actual=canonical_allowed(&path,root)?;
        if actual.is_file() {files.push(actual);}
        if files.len()>MAX_FILES {return Err(Error(-6,"glob has more than 10000 files".into()));}
    }
    files.sort();Ok(files)
}
fn parquet_reader(mut file:File) -> Result<SerializedFileReader<File>> {
    let size=file.metadata().map_err(io)?.len();
    if size<12 {return Err(invalid("invalid Parquet file length"));}
    file.seek(SeekFrom::End(-8)).map_err(io)?;let mut footer=[0;8];file.read_exact(&mut footer).map_err(io)?;
    let length=u32::from_le_bytes(footer[..4].try_into().map_err(io)?) as u64;
    if &footer[4..]!=b"PAR1" || length>8*1024*1024 || length+8>size {return Err(invalid("invalid or excessive Parquet footer"));}
    file.seek(SeekFrom::Start(0)).map_err(io)?;
    let reader=SerializedFileReader::new(file).map_err(io)?;
    let schema=reader.metadata().file_metadata().schema();
    if schema.get_fields().len()>MAX_COLUMNS || schema.get_fields().iter().any(|field|!field.is_primitive()
        || (field.get_basic_info().has_repetition() && field.get_basic_info().repetition()==parquet::basic::Repetition::REPEATED)) {return Err(unsupported("Parquet requires a flat schema with at most 1024 columns"));}
    for group in reader.metadata().row_groups() {
        if group.num_rows()<0 || group.total_byte_size()<0 {return Err(invalid("invalid Parquet row group size"));}
        if group.total_byte_size()>128*1024*1024 {return Err(Error(-6,"Parquet row group exceeds 128 MiB".into()));}
        for column in group.columns() {
            if column.compressed_size()<0 || column.uncompressed_size()<0
                || column.compressed_size() as u64>size || column.uncompressed_size()>128*1024*1024 {return Err(invalid("invalid or excessive Parquet column size"));}
        }
    }
    Ok(reader)
}
fn parquet_columns(reader:&SerializedFileReader<File>) -> Result<Vec<Column>> {
    reader.metadata().file_metadata().schema().get_fields().iter().map(|field| {
        let converted=field.get_basic_info().converted_type();
        let kind=match converted {
            ConvertedType::DATE=>Kind::Date,
            ConvertedType::TIMESTAMP_MILLIS|ConvertedType::TIMESTAMP_MICROS=>Kind::DateTime,
            ConvertedType::UTF8|ConvertedType::ENUM|ConvertedType::UINT_64=>Kind::Text,
            ConvertedType::DECIMAL|ConvertedType::TIME_MILLIS|ConvertedType::TIME_MICROS|ConvertedType::INTERVAL=>return Err(unsupported("unsupported Parquet logical type")),
            _=>match field.get_physical_type() {PhysicalType::BOOLEAN=>Kind::Boolean,PhysicalType::INT32|PhysicalType::INT64=>Kind::Integer,PhysicalType::FLOAT|PhysicalType::DOUBLE=>Kind::Double,PhysicalType::BYTE_ARRAY=>Kind::Text,_=>return Err(unsupported("unsupported Parquet physical/logical type"))},
        };
        Ok(Column{name:field.name().into(),kind,nullable:field.is_optional(),scale:if converted==ConvertedType::TIMESTAMP_MICROS {6} else if converted==ConvertedType::TIMESTAMP_MILLIS {3} else {0}})
    }).collect()
}
fn parquet_cell(field:&Field) -> Result<Cell> {
    let (text,kind)=match field {
        Field::Null=>return Ok(Cell::null()),Field::Bool(v)=>(v.to_string(),Kind::Boolean),
        Field::Byte(v)=>(v.to_string(),Kind::Integer),Field::Short(v)=>(v.to_string(),Kind::Integer),Field::Int(v)=>(v.to_string(),Kind::Integer),Field::Long(v)=>(v.to_string(),Kind::Integer),
        Field::UByte(v)=>(v.to_string(),Kind::Integer),Field::UShort(v)=>(v.to_string(),Kind::Integer),Field::UInt(v)=>(v.to_string(),Kind::Integer),Field::ULong(v)=>(v.to_string(),if *v<=i64::MAX as u64 {Kind::Integer} else {Kind::Text}),
        Field::Float(v) if v.is_finite()=>(v.to_string(),Kind::Double),Field::Double(v) if v.is_finite()=>(v.to_string(),Kind::Double),
        Field::Str(v)=>(v.clone(),Kind::Text),Field::Bytes(v)=>(std::str::from_utf8(v.data()).map_err(io)?.into(),Kind::Text),
        Field::Date(days)=> {let date=NaiveDate::from_ymd_opt(1970,1,1).and_then(|epoch|epoch.checked_add_signed(chrono::Duration::days(*days as i64))).ok_or_else(||invalid("Parquet date overflow"))?;(date.to_string(),Kind::Date)},
        Field::TimestampMillis(value)=> {let date=DateTime::from_timestamp_millis(*value).ok_or_else(||invalid("Parquet timestamp overflow"))?;(date.naive_utc().to_string(),Kind::DateTime)},
        Field::TimestampMicros(value)=> {let date=DateTime::from_timestamp_micros(*value).ok_or_else(||invalid("Parquet timestamp overflow"))?;(date.naive_utc().to_string(),Kind::DateTime)},
        _=>return Err(unsupported("unsupported or invalid Parquet value")),
    };
    if text.len()>MAX_ROW {return Err(Error(-6,"Parquet value exceeds 16 MiB".into()));}
    Ok(Cell{text,kind})
}

pub fn file_inspect(path:&str,format:&str,options:&str,root:&str) -> Box<FileSource> {
    let mut source=FileSource::empty(root);
    let result=(|| {
        let options:serde_json::Value=serde_json::from_str(options).map_err(io)?;
        let format=format.to_ascii_lowercase();
        let (actual,fingerprint)=if format=="glob" {(path.to_owned(),Fingerprint::default())} else {
            let (file,actual)=checked_open(path,root)?;(actual.to_string_lossy().into_owned(),Fingerprint::from(&file.metadata().map_err(io)?)?)
        };
        source.snapshot=Some(Snapshot{path:actual,format:format.clone(),options,columns:Vec::new(),fingerprint});
        source.open_backend()?;
        let mut columns=Vec::<Column>::new();
        if format=="parquet" {
            let (file,_)=checked_open(path,root)?;columns=parquet_columns(&parquet_reader(file)?)?;
        } else if format=="glob" {
            columns.push(Column{name:"file".into(),kind:Kind::Text,nullable:false,scale:0});
        } else {
            let mut count=0;
            while let Some(row)=source.raw_next()? {
                let present:HashSet<&str>=row.iter().map(|(name,_)|name.as_str()).collect();
                for column in &mut columns {column.nullable|=!present.contains(column.name.as_str());}
                for (name,value) in row {
                    if let Some(column)=columns.iter_mut().find(|column|column.name==name) {column.kind=merge(column.kind,value.kind);column.scale=column.scale.max(datetime_scale(&value));column.nullable|=value.kind==Kind::Null;}
                    else {let scale=datetime_scale(&value);columns.push(Column{name,kind:value.kind,nullable:count>0 || value.kind==Kind::Null,scale});}
                }
                count+=1;
                if columns.len()>MAX_COLUMNS {return Err(Error(-6,"more than 1024 inferred columns".into()));}
            }
            if columns.is_empty() {if let Some(Backend::Csv{names,..})=&source.backend {columns=names.iter().map(|name|Column{name:name.clone(),kind:Kind::Null,nullable:true,scale:0}).collect();}}
        }
        source.unchanged()?;
        source.snapshot.as_mut().expect("snapshot set above").columns=columns;
        source.open_backend()?;source.position=0;
        Ok(())
    })();
    if let Err(error)=result {source.fail(error);}
    Box::new(source)
}
pub fn file_resume(snapshot:&str,root:&str,projection:&[i32]) -> Box<FileSource> {
    let mut source=FileSource::empty(root);
    let result=(|| {
        let snapshot:Snapshot=serde_json::from_str(snapshot).map_err(io)?;
        let count=if describe(&snapshot) {6} else {snapshot.columns.len()};
        if projection.iter().any(|&index|index<0 || index as usize>=count) {return Err(invalid("invalid projected file column"));}
        source.snapshot=Some(snapshot);source.projection=projection.to_vec();source.open_backend()
    })();
    if let Err(error)=result {source.fail(error);}
    Box::new(source)
}

/// An exclusive temporary file is published only after the native query succeeds.
/// Keeping the parent directory open and using its /proc fd path on Linux also
/// prevents a renamed/symlinked directory from redirecting the final write.
pub struct FileExport {
    writer:Option<BufWriter<File>>, parent:Option<File>, temporary:PathBuf, target:PathBuf,
    delimiter:u8, quote:u8, escape:u8, nullstr:String, columns:usize,
    status:i32, error:String, finished:bool,
}
impl FileExport {
    fn fail(&mut self,error:Error) {self.status=error.0;self.error=error.1;self.writer=None;let _=std::fs::remove_file(&self.temporary);}
    pub fn copy_status(&self) -> i32 {self.status}
    pub fn copy_error(&self) -> String {self.error.clone()}
    pub fn copy_write(&mut self,cells:&[FileCell]) -> i32 {
        if self.status!=0 {return self.status;}
        let result=(|| {
            if self.finished || cells.len()!=self.columns {return Err(invalid("invalid COPY row width or closed writer"));}
            let mut output=Vec::new();
            for (index,cell) in cells.iter().enumerate() {
                if index>0 {output.push(self.delimiter);}
                let text=if cell.is_null {self.nullstr.as_bytes()} else {cell.text.as_bytes()};
                let quoted=!cell.is_null && (text==self.nullstr.as_bytes() || text.iter().any(|&ch|ch==self.delimiter || ch==self.quote || ch==b'\n' || ch==b'\r'));
                if text.len()>MAX_ROW {return Err(Error(-6,"COPY value exceeds 16 MiB".into()));}
                if quoted {output.push(self.quote);}
                for &ch in text {
                    if quoted && (ch==self.quote || (self.escape!=self.quote && ch==self.escape)) {output.push(self.escape);}
                    output.push(ch);
                }
                if quoted {output.push(self.quote);}
                if output.len()>MAX_ROW {return Err(Error(-6,"COPY record exceeds 16 MiB".into()));}
            }
            output.push(b'\n');
            self.writer.as_mut().ok_or_else(||invalid("COPY writer is closed"))?.write_all(&output).map_err(io)
        })();
        if let Err(error)=result {self.fail(error);}
        self.status
    }
    pub fn copy_finish(&mut self) -> i32 {
        if self.status!=0 || self.finished {return self.status;}
        let result=(|| {
            let mut writer=self.writer.take().ok_or_else(||invalid("COPY writer is closed"))?;
            writer.flush().map_err(io)?;writer.get_ref().sync_all().map_err(io)?;drop(writer);
            // hard_link is atomic and fails if target exists, including symlinks.
            std::fs::hard_link(&self.temporary,&self.target).map_err(io)?;
            self.finished=true;
            std::fs::remove_file(&self.temporary).map_err(io)
        })();
        if let Err(error)=result {self.fail(error);}
        self.status
    }
}
impl Drop for FileExport {fn drop(&mut self) {self.writer=None;let _=std::fs::remove_file(&self.temporary);}}
pub fn file_export(path:&str,options:&str,root:&str,headers:&[FileCell]) -> Box<FileExport> {
    static NEXT:AtomicU64=AtomicU64::new(0);
    let mut output=FileExport{writer:None,parent:None,temporary:PathBuf::new(),target:PathBuf::new(),delimiter:b',',quote:b'"',escape:b'"',nullstr:String::new(),columns:headers.len(),status:0,error:String::new(),finished:false};
    let result=(|| {
        let options:serde_json::Value=serde_json::from_str(options).map_err(io)?;
        output.delimiter=option_byte(&options,"delim",b',')?;
        output.quote=option_byte(&options,"quote",b'"')?;
        output.escape=option_byte(&options,"escape",output.quote)?;
        output.nullstr=options.get("nullstr").and_then(|value|value.as_str()).unwrap_or("").into();
        if output.delimiter==output.quote || output.delimiter==output.escape
            || output.nullstr.bytes().any(|ch|ch==output.delimiter || ch==output.quote || ch==b'\r' || ch==b'\n') {return Err(invalid("invalid COPY delimiter or NULL text"));}
        let target=Path::new(path);
        let name=target.file_name().ok_or_else(||invalid("COPY requires a file name"))?;
        let parent=canonical_allowed(target.parent().filter(|path|!path.as_os_str().is_empty()).unwrap_or(Path::new(".")),root)?;
        let directory=File::open(&parent).map_err(io)?;
        if !directory.metadata().map_err(io)?.is_dir() {return Err(invalid("COPY parent is not a directory"));}
        #[cfg(target_os="linux")]
        let parent={use std::os::fd::AsRawFd;
            let stable=PathBuf::from(format!("/proc/self/fd/{}",directory.as_raw_fd()));
            canonical_allowed(&stable,root)?;stable
        };
        output.parent=Some(directory);output.target=parent.join(name);
        if std::fs::symlink_metadata(&output.target).is_ok() {return Err(invalid("COPY target already exists"));}
        use std::fs::OpenOptions;
        for _ in 0..32 {
            let temporary=parent.join(format!(".seekdb-copy-{}-{}.tmp",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
            match OpenOptions::new().write(true).create_new(true).open(&temporary) {
                Ok(file)=>{output.temporary=temporary;output.writer=Some(BufWriter::new(file));break;},
                Err(error) if error.kind()==std::io::ErrorKind::AlreadyExists=>continue,
                Err(error)=>return Err(io(error)),
            }
        }
        if output.writer.is_none() {return Err(io("cannot create COPY temporary file"));}
        if headers.is_empty() || headers.len()>MAX_COLUMNS {return Err(invalid("invalid COPY column count"));}
        if options.get("header").and_then(|value|value.as_bool()).unwrap_or(false) {output.copy_write(headers);}
        Ok(())
    })();
    if let Err(error)=result {output.fail(error);}
    Box::new(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;
    static NEXT:AtomicU64=AtomicU64::new(0);
    struct Fixture(PathBuf);
    impl Fixture {
        fn new() -> Self {
            let path=std::env::temp_dir().join(format!("seekdb-file-sql-{}-{}",std::process::id(),NEXT.fetch_add(1,Ordering::Relaxed)));
            std::fs::create_dir(&path).unwrap();Self(path)
        }
        fn write(&self,name:&str,contents:&str) -> String {
            let path=self.0.join(name);std::fs::write(&path,contents).unwrap();path.to_string_lossy().into_owned()
        }
        fn root(&self) -> &str {self.0.to_str().unwrap()}
    }
    impl Drop for Fixture {fn drop(&mut self) {let _=std::fs::remove_dir_all(&self.0);}}
    #[test]
    fn csv_streaming_schema_quotes_and_rescan() {
        let f=Fixture::new();let path=f.write("data.csv","id,id,note,flag,day\r\n1,10,\"a,\nb\",true,2024-02-29\r\n2,20,\"say \"\"hi\"\"\",false,\r\n");
        let inspect=file_inspect(&path,"csv","{}",f.root());assert_eq!(0,inspect.file_status(),"{}",inspect.file_error());
        let cols=inspect.file_columns();assert_eq!("id__2",cols[1].name);assert_eq!(Kind::Boolean as u8,cols[3].kind);assert!(cols[4].nullable);
        let mut scan=file_resume(&inspect.file_snapshot(),f.root(),&[2,3]);
        assert_eq!(0,scan.file_next());assert_eq!("a,\nb",scan.file_row()[0].text);assert_eq!("1",scan.file_row()[1].text);
        assert_eq!(0,scan.file_next());assert_eq!("say \"hi\"",scan.file_row()[0].text);assert_eq!("0",scan.file_row()[1].text);
        assert_eq!(END,scan.file_next());assert_eq!(0,scan.file_rescan());assert_eq!(0,scan.file_next());
    }
    #[test]
    fn json_union_nulls_conflicts_and_nested_rejection() {
        let f=Fixture::new();let path=f.write("data.jsonl","{\"a\":1,\"b\":true}\n{\"a\":2.5,\"c\":\"2024-02-29\"}\n");
        let inspect=file_inspect(&path,"json","{}",f.root());assert_eq!(0,inspect.file_status(),"{}",inspect.file_error());
        let cols=inspect.file_columns();assert_eq!(3,cols.len());assert_eq!(Kind::Double as u8,cols[0].kind);assert!(cols[1].nullable && cols[2].nullable);
        let mut scan=file_resume(&inspect.file_snapshot(),f.root(),&[]);assert_eq!(0,scan.file_next());assert!(scan.file_row()[2].is_null);
        assert_eq!(0,scan.file_next());assert!(scan.file_row()[1].is_null);
        let bad=f.write("bad.jsonl","{\"nested\":{\"a\":1}}\n");assert_eq!(-2,file_inspect(&bad,"json","{}",f.root()).file_status());
    }
    #[test]
    fn integer_bounds_dates_and_type_widening() {
        assert_eq!(Kind::Integer,text_cell("9223372036854775807").kind);
        assert_eq!(Kind::Text,text_cell("9223372036854775808").kind);
        assert_eq!(Kind::Date,text_cell("2024-02-29").kind);
        assert_eq!(Kind::Text,text_cell("2023-02-29").kind);
        assert_eq!(Kind::DateTime,text_cell("2026-08-01 10:11:12").kind);
        assert_eq!(Kind::Text,text_cell("NaN").kind);
        assert_eq!(Kind::Double,merge(Kind::Integer,Kind::Double));
        assert_eq!(Kind::Text,merge(Kind::Boolean,Kind::Integer));
    }
    #[test]
    fn file_change_and_path_escape_are_rejected() {
        let f=Fixture::new();let outside=Fixture::new();let path=f.write("data.csv","id\n1\n");
        let inspect=file_inspect(&path,"csv","{}",f.root());assert_eq!(0,inspect.file_status());
        std::fs::write(&path,"id\n1\n2\n").unwrap();assert_eq!(-5,file_resume(&inspect.file_snapshot(),f.root(),&[]).file_status());
        let other=outside.write("other.csv","id\n1\n");assert_eq!(-4,file_inspect(&other,"csv","{}",f.root()).file_status());
        assert_eq!(-4,file_inspect(&path,"csv","{}","").file_status());
        #[cfg(unix)]
        {let link=f.0.join("escape.csv");std::os::unix::fs::symlink(&other,&link).unwrap();assert_eq!(-4,file_inspect(link.to_str().unwrap(),"csv","{}",f.root()).file_status());}
    }
    #[test]
    fn strict_csv_errors_and_quoted_nulls() {
        let f=Fixture::new();
        for bad in ["a,b\n1\n", "a\n\"unfinished", "a\n\"closed\"junk\n", "a\nun\"quoted\n"] {
            let path=f.write("bad.csv",bad);assert!(file_inspect(&path,"csv","{}",f.root()).file_status()<0,"{bad}");
        }
        let path=f.write("null.csv","a,b\n,\"\"\nNULL,\"NULL\"\n");
        let mut source=file_inspect(&path,"csv",r#"{"nullstr":["","NULL"]}"#,f.root());
        assert_eq!(0,source.file_status(),"{}",source.file_error());
        assert_eq!(0,source.file_next());assert!(source.file_row()[0].is_null);assert!(!source.file_row()[1].is_null);
        assert_eq!(0,source.file_next());assert!(source.file_row()[0].is_null);assert_eq!("NULL",source.file_row()[1].text);
        let path=f.write("large.csv",&format!("a\n{}", "x".repeat(MAX_ROW+1)));
        assert_eq!(-6,file_inspect(&path,"csv","{}",f.root()).file_status());
    }
    #[test]
    fn glob_describe_and_json_string_conflicts() {
        let f=Fixture::new();f.write("a.csv","id\n1\n");f.write("b.csv","id\n2\n");
        let mut paths=file_inspect(&format!("{}/*.csv",f.root()),"glob","{}",f.root());
        assert_eq!(0,paths.file_status(),"{}",paths.file_error());
        assert_eq!(0,paths.file_next());assert!(paths.file_row()[0].text.ends_with("a.csv"));
        assert_eq!(0,paths.file_next());assert_eq!(END,paths.file_next());assert_eq!(0,paths.file_rescan());
        let path=f.write("strings.jsonl","{\"id\":1,\"flag\":true}\n{\"id\":\"002\",\"flag\":\"false\"}\n");
        let inspected=file_inspect(&path,"json",r#"{"__describe":true}"#,f.root());
        assert_eq!(6,inspected.file_columns().len());
        let mut source=file_resume(&inspected.file_snapshot(),f.root(),&[0,1,2]);
        assert_eq!(0,source.file_next());assert_eq!("id",source.file_row()[0].text);assert_eq!("VARCHAR",source.file_row()[1].text);
        assert_eq!(0,source.file_next());assert_eq!("VARCHAR",source.file_row()[1].text);
    }
    #[test]
    fn parquet_snappy_multiple_groups_nulls_projection_and_rescan() {
        use parquet::{file::{writer::SerializedFileWriter,properties::WriterProperties},schema::parser::parse_message_type,data_type::{Int64Type,ByteArrayType,ByteArray},basic::Compression};
        use std::sync::Arc;
        let f=Fixture::new();let path=f.0.join("data.parquet");
        let schema=Arc::new(parse_message_type("message custom_root { REQUIRED INT64 id; OPTIONAL BINARY name (UTF8); }").unwrap());
        let props=Arc::new(WriterProperties::builder().set_compression(Compression::SNAPPY).set_dictionary_enabled(true).build());
        let mut writer=SerializedFileWriter::new(File::create(&path).unwrap(),schema,props).unwrap();
        for group_id in 0..2 {
            let mut group=writer.next_row_group().unwrap();
            let mut ids=group.next_column().unwrap().unwrap();
            ids.typed::<Int64Type>().write_batch(&[group_id*2,group_id*2+1],None,None).unwrap();ids.close().unwrap();
            let mut names=group.next_column().unwrap().unwrap();
            names.typed::<ByteArrayType>().write_batch(&[ByteArray::from("中文")],Some(&[1,0]),None).unwrap();names.close().unwrap();group.close().unwrap();
        }
        writer.close().unwrap();
        let source=file_inspect(path.to_str().unwrap(),"parquet","{}",f.root());
        assert_eq!(0,source.file_status(),"{}",source.file_error());assert!(source.file_columns()[1].nullable);
        let mut scan=file_resume(&source.file_snapshot(),f.root(),&[1]);
        assert_eq!(0,scan.file_next());assert_eq!("中文",scan.file_row()[0].text);
        assert_eq!(0,scan.file_next());assert!(scan.file_row()[0].is_null);
        assert_eq!(0,scan.file_next());assert_eq!(0,scan.file_next());assert_eq!(END,scan.file_next());
        assert_eq!(0,scan.file_rescan());assert_eq!(0,scan.file_next());
    }
    #[test]
    fn copy_roundtrip_exclusive_publish_and_abort_cleanup() {
        let f=Fixture::new();let path=f.0.join("export.csv");
        let cell=|text:&str,is_null|FileCell{text:text.into(),is_null};
        let headers=[cell("a",false),cell("b",false)];
        let options=r#"{"header":true,"nullstr":"NULL","delim":";","quote":"'","escape":"\\"}"#;
        let mut writer=file_export(path.to_str().unwrap(),options,f.root(),&headers);
        assert_eq!(0,writer.copy_status(),"{}",writer.copy_error());assert!(!path.exists());
        assert_eq!(0,writer.copy_write(&[cell("x;'\n中文\\",false),cell("",true)]));
        assert_eq!(0,writer.copy_write(&[cell("NULL",false),cell("",false)]));
        assert_eq!(0,writer.copy_finish(),"{}",writer.copy_error());
        let mut scan=file_inspect(path.to_str().unwrap(),"csv",options,f.root());
        assert_eq!(0,scan.file_status(),"{}",scan.file_error());assert_eq!(0,scan.file_next());
        assert_eq!("x;'\n中文\\",scan.file_row()[0].text);assert!(scan.file_row()[1].is_null);
        assert_eq!(0,scan.file_next());assert_eq!("NULL",scan.file_row()[0].text);assert!(!scan.file_row()[0].is_null);assert!(!scan.file_row()[1].is_null);
        assert!(file_export(path.to_str().unwrap(),options,f.root(),&headers).copy_status()<0);
        drop(writer);
        let aborted=f.0.join("aborted.csv");drop(file_export(aborted.to_str().unwrap(),options,f.root(),&headers));
        assert!(!aborted.exists());assert_eq!(1,std::fs::read_dir(&f.0).unwrap().count());
        let competing=f.0.join("race.csv");let mut writer=file_export(competing.to_str().unwrap(),options,f.root(),&headers);
        std::fs::write(&competing,"keep").unwrap();assert!(writer.copy_finish()<0);assert_eq!("keep",std::fs::read_to_string(&competing).unwrap());
        let outside=Fixture::new();assert_eq!(-4,file_export(outside.0.join("out.csv").to_str().unwrap(),options,f.root(),&headers).copy_status());
    }
}
