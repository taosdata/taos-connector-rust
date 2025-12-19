use std::env;
use std::process::Command;

const TIME_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.3f";

fn main() {
    let version = env::var("CARGO_PKG_VERSION").unwrap();
    let program = env::var("CARGO_PKG_NAME").unwrap();
    // let build_time = chrono::Local::now().format(TIME_FORMAT);
    let commit_id = Command::new("git")
        .args(&["rev-parse", "HEAD"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|_| "unknown".to_string());

    println!("cargo:rustc-env=VERSION={version}");
    println!("cargo:rustc-env=PROGRAM={program}",);
    // println!("cargo:rustc-env=BUILD_TIME={build_time}");
    println!("cargo:rustc-env=COMMIT_ID={commit_id}");
}
