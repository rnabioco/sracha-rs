use std::process::Command;

fn main() {
    let version = std::env::var("CARGO_PKG_VERSION").unwrap();
    let profile = std::env::var("PROFILE").unwrap_or_default();

    let sracha_version = if profile == "release" {
        version
    } else {
        let git_sha = Command::new("git")
            .args(["rev-parse", "--short", "HEAD"])
            .output()
            .ok()
            .filter(|o| o.status.success())
            .and_then(|o| {
                let sha = String::from_utf8(o.stdout).ok()?;
                let sha = sha.trim().to_string();
                if sha.is_empty() { None } else { Some(sha) }
            });

        match git_sha {
            Some(sha) => {
                let dirty = Command::new("git")
                    .args(["diff", "--quiet", "HEAD"])
                    .status()
                    .map(|s| !s.success())
                    .unwrap_or(false);

                if dirty {
                    format!("{version}-dev+{sha}.dirty")
                } else {
                    format!("{version}-dev+{sha}")
                }
            }
            None => format!("{version}-dev"),
        }
    };

    println!("cargo:rustc-env=SRACHA_VERSION={sracha_version}");

    // Rerun when git state changes (branch switch, commit, staging).
    // Paths are relative to the crate manifest directory (crates/sracha/).
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-changed=../../.git/index");
}
