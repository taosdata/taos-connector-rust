fn main() {
    let commit_id = std::process::Command::new("git")
        .args(&["rev-parse", "--short=7", "HEAD"])
        .output()
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|_| "ncid000".to_string());
    println!("cargo:rustc-env=GIT_COMMIT_ID={commit_id}");
}
