use std::cmp::Reverse;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread;
use std::time::Duration;

use chrono::{Local, NaiveTime, TimeDelta};
use flate2::write::GzEncoder;
use parking_lot::{RwLock, RwLockReadGuard};
use regex::Regex;
use snafu::{ensure, OptionExt, ResultExt};
use sysinfo::Disks;
use tracing::Level;

use crate::{
    CompressSnafu, CreateLogDirSnafu, DiskMountPointNotFoundSnafu, GetFileSizeSnafu,
    GetLogAbsolutePathSnafu, InvalidRotationSizeSnafu, OpenLogFileSnafu, ReadDirSnafu, Result,
};

const TAOS_LOG_00: &str = "taoslog0.0";
const TAOS_LOG_01: &str = "taoslog0.1";

#[derive(Debug, Clone)]
struct Rotation {
    /// file size in bytes
    file_size: u64,
}

struct State {
    current_index: u8,
    file_path: PathBuf,
}

pub struct RollingFileAppenderBuilder<'a> {
    log_dir: PathBuf,
    component_name: String,
    instance_id: u8,
    rotation_count: usize,
    log_keep_days: TimeDelta,
    rotation_size: &'a str,
    compress: bool,
    reserved_disk_size: &'a str,
    stop_logging_threshold: usize,
}

impl<'a> RollingFileAppenderBuilder<'a> {
    pub fn rotation_count(self, rotation_count: u16) -> Self {
        Self {
            rotation_count: rotation_count as usize,
            ..self
        }
    }

    pub fn keep_days(self, log_keep_days: u16) -> Self {
        Self {
            log_keep_days: TimeDelta::days(log_keep_days as _),
            ..self
        }
    }

    pub fn rotation_size(self, rotation_size: &'a str) -> Self {
        Self {
            rotation_size,
            ..self
        }
    }

    pub fn compress(self, compress: bool) -> Self {
        Self { compress, ..self }
    }

    pub fn reserved_disk_size(self, reserved_disk_size: &'a str) -> Self {
        Self {
            reserved_disk_size,
            ..self
        }
    }

    pub fn stop_logging_threadhold(self, stop_logging_threshold: usize) -> Self {
        Self {
            stop_logging_threshold,
            ..self
        }
    }

    pub fn build(mut self) -> Result<RollingFileAppender> {
        if !self.log_dir.is_absolute() {
            self.log_dir = self
                .log_dir
                .canonicalize()
                .context(GetLogAbsolutePathSnafu)?;
        }
        if !self.log_dir.is_dir() {
            fs::create_dir_all(&self.log_dir).context(CreateLogDirSnafu {
                path: &self.log_dir,
            })?;
        }

        let (cur_idx, file_path) = determine_current_file(&self.log_dir)?;
        dbg!(
            "determine_current_file, file_path: {:?}, cur_idx: {}",
            &file_path,
            &cur_idx
        );
        let file = create_or_open_file(&file_path)?;
        // match create_file(&file_path)? {
        //     Some(file) => break (file_path, file),
        //     None => {}
        // }
        // }
        // };

        // Current max seq id
        // let mut max_seq_id =
        //     today_max_seq_id(&self.component_name, self.instance_id, &self.log_dir)?;

        // Init log file
        // let now = Local::now();
        // let today = time_format(now);
        // // TODO
        // let (file_path, file) = loop {
        //     let filename = if max_seq_id == 0 {
        //         format!(
        //             "{}_{}_{}.log",
        //             &self.component_name, self.instance_id, today
        //         )
        //     } else {
        //         format!(
        //             "{}_{}_{}.log.{}",
        //             &self.component_name, self.instance_id, today, max_seq_id
        //         )
        //     };
        //     let file_path = self.log_dir.join(&filename);
        //     match create_or_open_file(&file_path)? {
        //         Some(file) => break (file_path, file),
        //         None => max_seq_id += 1,
        //     }
        // };

        // Next rotate time
        let rotation = Rotation {
            file_size: parse_unit_size(self.rotation_size)?,
        };

        let state = State {
            current_index: cur_idx, // FIXME
            file_path,
        };

        // Calc disk available space
        let mut disks = Disks::new();
        disks.refresh_list();
        let mut disks = Vec::from(disks);
        disks.sort_by_key(|d| Reverse(d.mount_point().to_str().map(|s| s.len())));
        let mut disk = disks
            .into_iter()
            .find(|d| self.log_dir.starts_with(d.mount_point()))
            .context(DiskMountPointNotFoundSnafu)?;
        disk.refresh();
        let disk_available_space = Arc::new(AtomicU64::new(disk.available_space()));
        thread::spawn({
            let disk_available_space = disk_available_space.clone();
            move || loop {
                disk.refresh();
                disk_available_space.store(disk.available_space(), Ordering::SeqCst);
                thread::sleep(Duration::from_secs(30));
            }
        });

        let (event_tx, event_rx) = flume::bounded(1);

        let config = Config {
            log_dir: self.log_dir,
            instance_id: self.instance_id,
            rotation,
            reserved_disk_size: parse_unit_size(self.reserved_disk_size)?,
            compress: self.compress,
            component_name: self.component_name,
            rotation_count: self.rotation_count,
            log_keep_days: self.log_keep_days,
            stop_logging_threshold: self.stop_logging_threshold as f64 / 100f64,
        };

        thread::spawn({
            let config = config.clone();
            move || {
                while event_rx.recv().is_ok() {
                    config.handle_old_files().ok();
                }
            }
        });

        // Handle old files
        event_tx.send(()).ok();

        Ok(RollingFileAppender {
            config,
            disk_available_space,
            level_downgrade: AtomicBool::default(),
            event_tx,
            state: RwLock::new(state),
            writer: RwLock::new(file),
        })
    }
}

pub struct RollingFileAppender {
    config: Config,
    disk_available_space: Arc<AtomicU64>,
    level_downgrade: AtomicBool,
    event_tx: flume::Sender<()>,
    state: RwLock<State>,
    writer: RwLock<File>,
}

impl RollingFileAppender {
    pub fn builder<'a, P: AsRef<Path>, S: Into<String>>(
        log_dir: P,
        component: S,
        instance_id: u8,
    ) -> RollingFileAppenderBuilder<'a> {
        RollingFileAppenderBuilder {
            log_dir: log_dir.as_ref().to_path_buf(),
            rotation_count: 30,
            log_keep_days: TimeDelta::days(30),
            rotation_size: "1GB",
            compress: false,
            reserved_disk_size: "2GB",
            component_name: component.into(),
            instance_id,
            stop_logging_threshold: 50,
        }
    }

    // TODO: modify Result<File>
    fn rotate(&self) -> Result<Option<File>> {
        let mut state = self.state.write();
        // tracing::info!("rotate log file");

        // Rotate by time
        // if let Some(file) = self.check_today_file_exists(&mut state)? {
        //     return Ok(Some(file));
        // }

        // Rotate by size
        // let now = Local::now();
        let cur_size = self
            .writer
            .read()
            .metadata()
            .context(GetFileSizeSnafu {
                path: &state.file_path,
            })?
            .len();

        if cur_size >= self.config.rotation.file_size {
            // Create a new file
            // state.max_seq_id += 1;

            // let (old_filename, new_filename, file) = loop {
            // TODO: modify filename
            let (old_filename, new_filename) = if state.current_index == 0 {
                ("taoslog0.0", "taoslog0.1")
            } else {
                ("taoslog0.1", "taoslog0.0")
            };

            // let filename = format!(
            //     "{}_{}_{}.log.{}",
            //     self.config.component_name,
            //     self.config.instance_id,
            //     time_format(now),
            //     // state.max_seq_id
            //     0
            // );
            let old_filename = self.config.log_dir.join(old_filename);
            let new_filename = self.config.log_dir.join(new_filename);
            let l = format!(
                "rotate log file, old: {:?}, new: {:?}",
                &old_filename, &new_filename
            );
            dbg!(l);
            let file = create_or_open_file(&new_filename)?;
            // match create_file(&new_filename)? {
            //     Some(file) => break (old_filename, new_filename, file),
            //     // None => state.max_seq_id += 1,
            //     None => {}
            // }
            // };

            // compress old file if needed
            // TODO: add compress
            // if self.compress {
            compress(old_filename).ok();
            // }

            // TODO: confim?
            // Handle old files
            self.event_tx.try_send(()).ok();
            state.current_index = 1 - state.current_index;
            state.file_path = new_filename;
            return Ok(Some(file));
        }

        // TODO: confirm
        // The current file is accidentally deleted
        if !state.file_path.is_file() {
            let filename = if state.current_index == 0 {
                "taoslog0.0"
            } else {
                "taoslog0.1"
            };
            let filename = self.config.log_dir.join(filename);
            // state.current_index
            state.file_path = filename.clone();
            dbg!("rolling file not found, create new file: {:?}", &filename);
            let file = create_or_open_file(filename)?;
            return Ok(Some(file));
        }
        // let mut max_seq_id = today_max_seq_id(
        //     &self.config.component_name,
        //     self.config.instance_id,
        //     &self.config.log_dir,
        // )?;
        // loop {
        //     let filename = if max_seq_id == 0 {
        //         format!(
        //             "{}_{}_{}.log",
        //             self.config.component_name,
        //             self.config.instance_id,
        //             time_format(now)
        //         )
        //     } else {
        //         format!(
        //             "{}_{}_{}.log.{}",
        //             self.config.component_name,
        //             self.config.instance_id,
        //             time_format(now),
        //             max_seq_id
        //         )
        //     };
        //     let filename = self.config.log_dir.join(filename);
        //     match create_file(&filename)? {
        //         Some(file) => {
        //             // state.max_seq_id = max_seq_id;
        //             state.file_path = filename;
        //             return Ok(Some(file));
        //         }
        //         None => max_seq_id += 1,
        //     }
        // }
        // }

        Ok(None)
    }

    // fn check_today_file_exists(&self, state: &mut State) -> Result<Option<File>> {
    //     // Check curent file name first
    //     let today = time_format(Local::now());
    //     let filename = state
    //         .file_path
    //         .file_name()
    //         .and_then(|f| f.to_str())
    //         .expect("file name not found in state file path");
    //     let (date, _) = parse_filename(
    //         &self.config.component_name,
    //         self.config.instance_id,
    //         filename,
    //     )
    //     .expect("filename should include date and index");
    //     if date == Local::now().with_time(NaiveTime::MIN).unwrap() {
    //         return Ok(None);
    //     }

    //     // today max seq id
    //     let max_seq_id = today_max_seq_id(
    //         &self.config.component_name,
    //         self.config.instance_id,
    //         &self.config.log_dir,
    //     )?;

    //     // init log file
    //     if max_seq_id != 0 {
    //         return Ok(None);
    //     }
    //     let filename = format!(
    //         "{}_{}_{}.log",
    //         &self.config.component_name, self.config.instance_id, today
    //     );
    //     let file_path = self.config.log_dir.join(&filename);
    //     match create_file(&file_path)? {
    //         Some(file) => {
    //             self.event_tx.try_send(()).ok();
    //             state.file_path = file_path;
    //             // state.max_seq_id = max_seq_id;
    //             Ok(Some(file))
    //         }
    //         None => Ok(None),
    //     }
    // }
}

fn determine_current_file(log_dir: impl AsRef<Path>) -> Result<(u8, PathBuf)> {
    let log_dir = log_dir.as_ref();
    let path0 = log_dir.join(TAOS_LOG_00);
    let path1 = log_dir.join(TAOS_LOG_01);
    match (path0.exists(), path1.exists()) {
        (_, false) => Ok((0, path0.clone())),
        (false, true) => Ok((1, path1.clone())),
        (true, true) => {
            let modified0 = fs::metadata(&path0)
                .context(OpenLogFileSnafu { path: &path0 })?
                .modified()
                .context(OpenLogFileSnafu { path: &path0 })?;
            let modified1 = fs::metadata(&path1)
                .context(OpenLogFileSnafu { path: &path1 })?
                .modified()
                .context(OpenLogFileSnafu { path: &path1 })?;
            if modified0 > modified1 {
                Ok((0, path0))
            } else {
                Ok((1, path1))
            }
        }
    }
}

// fn today_max_seq_id(
//     component_name: &str,
//     instance_id: u8,
//     log_dir: impl AsRef<Path>,
// ) -> Result<usize> {
//     let log_dir = log_dir.as_ref();
//     Ok(fs::read_dir(log_dir)
//         .context(ReadDirSnafu { path: log_dir })?
//         .filter_map(|entry| {
//             let entry = entry.ok()?;
//             let metadata = entry.metadata().ok()?;
//             if !metadata.is_file() {
//                 return None;
//             }

//             let filename = entry.file_name().to_str()?.to_string();
//             let res = parse_filename(component_name, instance_id, &filename)?;
//             (res.0 == Local::now().with_time(NaiveTime::MIN).unwrap()).then_some(res.1)
//         })
//         .max()
//         .unwrap_or_default())
// }

#[derive(Debug, Clone)]
struct Config {
    log_dir: PathBuf,
    component_name: String,
    instance_id: u8,
    rotation: Rotation,
    reserved_disk_size: u64,
    compress: bool,
    rotation_count: usize,
    log_keep_days: TimeDelta,
    stop_logging_threshold: f64,
}

impl Config {
    fn handle_old_files(&self) -> Result<()> {
        // Delete redundant old files
        let mut files = fs::read_dir(&self.log_dir)
            .context(ReadDirSnafu {
                path: &self.log_dir,
            })?
            .filter_map(|entry| {
                let entry = entry.ok()?;
                let metadata = entry.metadata().ok()?;
                if !metadata.is_file() {
                    return None;
                }

                let filename = entry.file_name().to_str()?.to_string();
                if is_active_log_file(&filename) {
                    return None;
                }

                let ts = parse_filename(&filename)?;
                // let res = parse_filename(&self.component_name, self.instance_id, &filename)?;
                Some((filename, ts))
            })
            // .collect::<Vec<(String, (DateTime<Local>, usize))>>();
            // .collect::<Vec<(String, i64)>>();
            .collect::<Vec<_>>();

        // files.sort_by(|(_, a), (_, b)| filename_cmp(a, b));
        files.sort_by(|a, b| b.1.cmp(&a.1));

        // Remove the newly created file without processing it
        // files.pop();
        // files.reverse();

        // if files.is_empty() {
        //     return Ok(());
        // }

        // TODO: optimize
        // 1. index
        // 2. into_par_iter
        if self.rotation_count > 0 && files.len() > self.rotation_count {
            let files_to_delete = &files[self.rotation_count..];
            files_to_delete.iter().for_each(|(filename, _)| {
                fs::remove_file(self.log_dir.join(filename)).ok();
            });
        }

        if !self.log_keep_days.is_zero() {
            let reserved_time =
                Local::now().with_time(NaiveTime::MIN).unwrap() - self.log_keep_days;
            let cutoff_ts = reserved_time.timestamp();
            files
                .iter()
                .filter(|(_, ts)| *ts < cutoff_ts)
                .for_each(|(filename, _)| {
                    fs::remove_file(self.log_dir.join(filename)).ok();
                });
        }

        // let mut split_index = files.len();

        // // delete by rotation_count
        // if self.rotation_count != 0 {
        //     split_index = self.rotation_count.saturating_sub(1).min(split_index);
        // }

        // if !self.log_keep_days.is_zero() {
        //     // delete by log_kep_days
        //     let reserved_time =
        //         Local::now().with_time(NaiveTime::MIN).unwrap() - self.log_keep_days;
        //     let index = files.partition_point(|(_, (date, _))| date > &reserved_time);
        //     split_index = split_index.min(index);
        // }

        // // split files for compress and delete
        // let (compress_files, delete_files) = files.split_at(split_index);

        // compress
        // if self.compress {
        //     compress_files
        //         .into_par_iter()
        //         .filter(|(filename, _)| !filename.ends_with(".gz"))
        //         .for_each(|(filename, _)| {
        //             compress(self.log_dir.join(filename)).ok();
        //         });
        // }

        // delete_files.into_par_iter().for_each(|(filename, _)| {
        //     fs::remove_file(self.log_dir.join(filename)).ok();
        // });

        Ok(())
    }
}

pub struct RollingWriter<'a>(RwLockReadGuard<'a, File>);

impl std::io::Write for RollingWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        (&*self.0).write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        (&*self.0).flush()
    }
}

pub enum TaosLogWriter<'a> {
    Rolling(RollingWriter<'a>),
    Null(std::io::Empty),
}

impl std::io::Write for TaosLogWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            TaosLogWriter::Rolling(w) => w.write(buf),
            TaosLogWriter::Null(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            TaosLogWriter::Rolling(w) => w.flush(),
            TaosLogWriter::Null(w) => w.flush(),
        }
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for RollingFileAppender {
    type Writer = TaosLogWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        if let Ok(Some(file)) = self.rotate() {
            *self.writer.write() = file;
        }
        TaosLogWriter::Rolling(RollingWriter(self.writer.read()))
    }

    fn make_writer_for(&'a self, meta: &tracing::Metadata<'_>) -> Self::Writer {
        let level = meta.level();
        let current_disk_space = self.disk_available_space.load(Ordering::SeqCst);
        if current_disk_space as f64 / self.config.reserved_disk_size as f64
            <= self.config.stop_logging_threshold
        {
            return TaosLogWriter::Null(std::io::empty());
        }

        let level_downgrade = current_disk_space <= self.config.reserved_disk_size;
        if level_downgrade
            && self
                .level_downgrade
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok_and(|x| !x)
        {
            let mut writer = self.make_writer();
            writer
                .write_all(b"========== level downgrade ==========\n")
                .ok();
            writer.flush().ok();
        }
        if !level_downgrade
            && self
                .level_downgrade
                .compare_exchange(true, false, Ordering::AcqRel, Ordering::Acquire)
                .is_ok_and(|x| x)
        {
            let mut writer = self.make_writer();
            writer
                .write_all(b"========== level upgrade ==========\n")
                .ok();
            writer.flush().ok();
        }
        if level_downgrade && level > &Level::ERROR {
            TaosLogWriter::Null(std::io::empty())
        } else {
            self.make_writer()
        }
    }
}

// fn time_format<'a>(datetime: DateTime<Local>) -> DelayedFormat<StrftimeItems<'a>> {
//     datetime.date_naive().format(DATE_FORMAT)
// }

// fn create_file(name: impl AsRef<Path>) -> Result<Option<File>> {
//     let path = name.as_ref();
//     match fs::OpenOptions::new()
//         .append(true)
//         .create_new(true)
//         .open(path)
//     {
//         Ok(file) => Ok(Some(file)),
//         Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(None),
//         e @ Err(_) => Ok(Some(e.context(OpenLogFileSnafu { path })?)),
//     }
// }

fn create_or_open_file(name: impl AsRef<Path>) -> Result<File> {
    let path = name.as_ref();
    dbg!("create_or_open_file, name: {:?}", path);
    // tracing::info!("create_or_open_file, name: {:?}", path);
    fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .context(OpenLogFileSnafu { path })
}

fn compress(path: impl AsRef<Path>) -> Result<()> {
    let ts = Local::now().timestamp();
    let compressed_name = format!("taoslog.{}.gz", ts);
    let path = path.as_ref();
    let dest_path = path.parent().unwrap().join(compressed_name);

    // let dest_path = PathBuf::from(format!("{}.gz", path.display()));
    // dbg!("dest_path:", &dest_path, "path:", &path);

    let mut src_file = File::open(path).context(CompressSnafu { path })?;
    // TODO: File::create(dest_path)
    let dest_file = match fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&dest_path)
    {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            dbg!("compress file already exists, remove it and retry");
            return Ok(());
        }
        e @ Err(_) => e.context(OpenLogFileSnafu { path })?,
    };

    let mut encoder = GzEncoder::new(dest_file, flate2::Compression::default());
    std::io::copy(&mut src_file, &mut encoder).context(CompressSnafu { path })?;
    fs::remove_file(path).context(CompressSnafu { path })?;
    Ok(())
}

fn parse_filename(filename: &str) -> Option<i64> {
    static RE: OnceLock<Regex> = OnceLock::new();
    let re = RE.get_or_init(|| Regex::new(r"^taoslog\.(?<timestamp>\d+)\.gz$").unwrap());
    let caps = re.captures(filename)?;
    caps.name("timestamp")?.as_str().parse().ok()
}

fn is_active_log_file(filename: &str) -> bool {
    filename == "taoslog0.0" || filename == "taoslog0.1"
}

// fn parse_filename(
//     component: &str,
//     instance_id: u8,
//     filename: &str,
// ) -> Option<(DateTime<Local>, usize)> {
//     static LOG_FILE_NAME_RE: OnceLock<Regex> = OnceLock::new();
//     let re = LOG_FILE_NAME_RE.get_or_init(|| {
//         let re = r"(?<date>\d{8})\.log(\.(?<index1>\d+)|\.gz|\.(?<index2>\d+)\.gz)?$";
//         Regex::new(&format!("^{component}_{instance_id}_{re}")).unwrap()
//     });
//     let caps = re.captures(filename)?;
//     let date = caps.name("date").and_then(|m| parse_date_str(m.as_str()))?;
//     let index = caps
//         .name("index1")
//         .or(caps.name("index2"))
//         .and_then(|m| m.as_str().parse().ok())
//         .unwrap_or_default();
//     Some((date, index))
// }

// fn parse_date_str(date: &str) -> Option<DateTime<Local>> {
//     let dt = NaiveDateTime::parse_from_str(&format!("{date} 000000"), DATE_TIME_FORMAT).ok()?;
//     Local.from_local_datetime(&dt).single()
// }

fn parse_unit_size(size: &str) -> Result<u64> {
    ensure!(size.len() >= 3, InvalidRotationSizeSnafu { size });
    ensure!(size.is_ascii(), InvalidRotationSizeSnafu { size });
    let (count, unit) = size.split_at(size.len() - 2);
    let count = count
        .parse::<u64>()
        .ok()
        .context(InvalidRotationSizeSnafu { size })?;
    match unit.to_ascii_uppercase().as_str() {
        "KB" => Ok(count * 1024),
        "MB" => Ok(count * 1024 * 1024),
        "GB" => Ok(count * 1024 * 1024 * 1024),
        _ => InvalidRotationSizeSnafu { size }.fail(),
    }
}

// fn filename_cmp(a: &(DateTime<Local>, usize), b: &(DateTime<Local>, usize)) -> cmp::Ordering {
//     match a.0.cmp(&b.0) {
//         cmp::Ordering::Equal => a.1.cmp(&b.1),
//         p => p,
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn parse_filename_test() {
//         let component = "taosx";

//         assert_eq!(
//             parse_filename(component, 1, "taosx_1_20240909.log"),
//             Some((parse_date_str("20240909").unwrap(), 0))
//         );
//         assert_eq!(
//             parse_filename(component, 2, "taosx_1_20240909.log.1"),
//             Some((parse_date_str("20240909").unwrap(), 1))
//         );
//         assert_eq!(
//             parse_filename(component, 3, "taosx_1_20240909.log.gz"),
//             Some((parse_date_str("20240909").unwrap(), 0))
//         );
//         assert_eq!(
//             parse_filename(component, 4, "taosx_1_20240909.log.1.gz"),
//             Some((parse_date_str("20240909").unwrap(), 1))
//         );
//         assert_eq!(
//             parse_filename(component, 1, "taosx_agent_1_20240909.log"),
//             None
//         );
//         assert_eq!(
//             parse_filename(component, 1, "taosx_agent_1_20240909.log"),
//             None
//         );
//     }

//     #[test]
//     fn time_format_test() {
//         let dt_str = "20250626";
//         assert_eq!(
//             time_format(parse_date_str(dt_str).unwrap()).to_string(),
//             "20250626"
//         );
//     }

//     #[test]
//     fn parse_unit_size_test() {
//         assert_eq!(parse_unit_size("5KB").unwrap(), 5 * 1024);
//         assert_eq!(parse_unit_size("5MB").unwrap(), 5 * 1024 * 1024);
//         assert_eq!(parse_unit_size("5GB").unwrap(), 5 * 1024 * 1024 * 1024);

//         assert!(parse_unit_size("5GBK").is_err());
//         assert!(parse_unit_size("GB").is_err());
//     }

//     #[test]
//     fn filename_cmp_test() {
//         assert_eq!(
//             filename_cmp(
//                 &(parse_date_str("20240909").unwrap(), 1),
//                 &(parse_date_str("20240909").unwrap(), 1)
//             ),
//             cmp::Ordering::Equal
//         );
//         assert_eq!(
//             filename_cmp(
//                 &(parse_date_str("20240909").unwrap(), 2),
//                 &(parse_date_str("20240909").unwrap(), 1)
//             ),
//             cmp::Ordering::Greater
//         );
//         assert_eq!(
//             filename_cmp(
//                 &(parse_date_str("20240909").unwrap(), 1),
//                 &(parse_date_str("20240909").unwrap(), 2)
//             ),
//             cmp::Ordering::Less
//         );

//         assert_eq!(
//             filename_cmp(
//                 &(parse_date_str("20240910").unwrap(), 1),
//                 &(parse_date_str("20240909").unwrap(), 1)
//             ),
//             cmp::Ordering::Greater
//         );
//         assert_eq!(
//             filename_cmp(
//                 &(parse_date_str("20240910").unwrap(), 2),
//                 &(parse_date_str("20240909").unwrap(), 1)
//             ),
//             cmp::Ordering::Greater
//         );
//         assert_eq!(
//             filename_cmp(
//                 &(parse_date_str("20240910").unwrap(), 0),
//                 &(parse_date_str("20240909").unwrap(), 1)
//             ),
//             cmp::Ordering::Greater
//         );

//         assert_eq!(
//             filename_cmp(
//                 &(parse_date_str("20240909").unwrap(), 1),
//                 &(parse_date_str("20240910").unwrap(), 1)
//             ),
//             cmp::Ordering::Less
//         );
//         assert_eq!(
//             filename_cmp(
//                 &(parse_date_str("20240909").unwrap(), 2),
//                 &(parse_date_str("20240910").unwrap(), 1)
//             ),
//             cmp::Ordering::Less
//         );
//         assert_eq!(
//             filename_cmp(
//                 &(parse_date_str("20240909").unwrap(), 0),
//                 &(parse_date_str("20240910").unwrap(), 1)
//             ),
//             cmp::Ordering::Less
//         );
//     }
// }
