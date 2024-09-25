#![allow(dead_code)]

use std::env;

use xshell::{cmd, Shell};

fn main() {
    if let Err(e) = try_main() {
        eprintln!("{}", e);
        std::process::exit(-1);
    }
}

type DynError = Box<dyn std::error::Error>;
type Result<T = ()> = std::result::Result<T, DynError>;

fn try_main() -> Result {
    let mut tasks = env::args().skip(1);
    while let Some(task) = tasks.next().as_deref() {
        match task {
            "msrv" => update_msrv_lock()?,
            "min" => update_min_lock()?,
            _ => {
                print_help();
                return Ok(());
            }
        }
    }
    Ok(())
}

fn print_help() {
    eprintln!(
        "Tasks:

msrv            Update the MSRV lockfile
min             Update the MIN lockfile
"
    )
}

fn update_msrv_lock() -> Result {
    let Env {
        lockfile,
        ci_dir,
        dry_run,
        ..
    } = task_env();

    let sh = Shell::new()?;
    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".into());

    let msrv = {
        let metadata = cmd!(sh, "{cargo} metadata --no-deps --format-version=1").read()?;
        let package = "neo4rs";

        cmd!(
            sh,
            "jq --raw-output '.packages[] | select(.name == \"'{package}'\") | .rust_version'"
        )
        .stdin(metadata)
        .read()
    }?;

    cmd!(sh, "rm {lockfile}").run_if(dry_run)?;

    let pin_versions: [(String, &str); 0] = [];
    for (krate, version) in pin_versions {
        pin_version(dry_run, &sh, &cargo, &krate, version)?;
    }

    cmd!(sh, "cargo +{msrv} test --no-run --all-features").run_if(dry_run)?;

    cmd!(sh, "cp {lockfile} {ci_dir}/Cargo.lock.msrv").run_if(dry_run)?;

    Ok(())
}

fn update_min_lock() -> Result {
    let Env {
        lockfile,
        ci_dir,
        dry_run,
        ..
    } = task_env();

    let sh = Shell::new()?;
    let cargo = env::var("CARGO").unwrap_or_else(|_| "cargo".into());

    cmd!(sh, "rm {lockfile}").run_if(dry_run)?;

    pin_version(dry_run, &sh, &cargo, "ahash", "0.8.7")?;

    cmd!(
        sh,
        "cargo +nightly -Z minimal-versions test --no-run --all-features"
    )
    .env("RUST_LOG", "debug")
    .run_if(dry_run)?;

    cmd!(sh, "cp {lockfile} {ci_dir}/Cargo.lock.min").run_if(dry_run)?;

    Ok(())
}

fn pin_version(dry_run: bool, sh: &Shell, cargo: &str, krate: &str, version: &str) -> Result<()> {
    cmd!(sh, "{cargo} update --package {krate} --precise {version}").run_if(dry_run)?;
    Ok(())
}

fn latest_version(sh: &Shell, krate: &str) -> Result<String> {
    let index = match krate.len() {
        1 => format!("https://index.crates.io/1/{}", krate),
        2 => format!("https://index.crates.io/2/{}", krate),
        3 => format!("https://index.crates.io/3/{}/{}", &krate[..1], krate),
        _ => format!(
            "https://index.crates.io/{}/{}/{}",
            &krate[..2],
            &krate[2..4],
            krate
        ),
    };

    let index = cmd!(sh, "curl --silent {index}").read()?;

    let version = cmd!(sh, "jq --slurp --raw-output 'map(.vers) | last'")
        .stdin(index)
        .read()?;

    Ok(format!("{krate}@{version}"))
}

struct Env {
    lockfile: String,
    ci_dir: String,
    dry_run: bool,
}

fn task_env() -> Env {
    let dry_run = env::var("DRY_RUN").is_ok();

    let workspace = env::var("WORKSPACE_ROOT");

    let (lockfile, ci_dir) = match workspace {
        Ok(ws) => (format!("{}/Cargo.lock", ws), format!("{}/ci", ws)),
        Err(_) => ("Cargo.lock".into(), "ci".into()),
    };

    Env {
        lockfile,
        ci_dir,
        dry_run,
    }
}

trait DryRun {
    fn run_if(&self, dry_run: bool) -> Result<()>;
}

impl DryRun for xshell::Cmd<'_> {
    fn run_if(&self, dry_run: bool) -> Result<()> {
        if dry_run {
            eprintln!("DRY_RUN: {}", self);
        } else {
            self.run()?;
        }
        Ok(())
    }
}
