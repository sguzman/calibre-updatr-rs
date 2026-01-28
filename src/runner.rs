use crate::config::CalibreEnvMode;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::ffi::OsStr;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::thread;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};
use wait_timeout::ChildExt;

const CALIBRE_ENVS: &[&[(&str, &str)]] = &[
    &[
        ("LC_ALL", "en_US.utf8"),
        ("LANG", "en_US.utf8"),
        ("LANGUAGE", "en_US:en"),
        ("CALIBRE_OVERRIDE_LANG", "en"),
    ],
    &[
        ("LC_ALL", "C.utf8"),
        ("LANG", "C.utf8"),
        ("LANGUAGE", "en"),
        ("CALIBRE_OVERRIDE_LANG", "en"),
    ],
    &[
        ("LC_ALL", "C"),
        ("LANG", "C"),
        ("LANGUAGE", "en"),
        ("CALIBRE_OVERRIDE_LANG", "en"),
    ],
];

#[derive(Debug)]
pub struct CmdResult {
    pub status_code: i32,
    pub stdout: String,
    pub stderr: String,
    pub timed_out: bool,
}

#[derive(Debug)]
pub struct Runner {
    pub calibredb_env_mode: CalibreEnvMode,
    pub debug_calibredb_env: bool,
    pub headless_fetch: bool,
    pub headless_env: HashMap<String, String>,
    pub fetch_use_xvfb: bool,
    pub calibre_username: Option<String>,
    pub calibre_password: Option<String>,
}

fn is_calibredb(cmd0: &str) -> bool {
    Path::new(cmd0)
        .file_name()
        .and_then(OsStr::to_str)
        .map(|s| s == "calibredb")
        .unwrap_or(false)
}

fn trim_if_present(s: &str) -> String {
    s.trim().to_string()
}

fn truncate(s: &str, max: usize) -> String {
    s.chars().take(max).collect()
}

fn should_clean_env_key(key: &str) -> bool {
    key.starts_with("PYTHON")
        || key.starts_with("VIRTUAL_ENV")
        || key.starts_with("UV_")
        || key.starts_with("PIP_")
        || key.starts_with("CONDA")
        || key.starts_with("POETRY")
        || key.starts_with("PYENV")
}

fn base_env_with_extra(extra_env: Option<&HashMap<String, String>>) -> HashMap<String, String> {
    let mut base_env: HashMap<String, String> = std::env::vars().collect();
    if let Some(extra) = extra_env {
        for (k, v) in extra {
            base_env.insert(k.clone(), v.clone());
        }
    }
    base_env
}

impl Runner {
    pub fn run(
        &self,
        cmd: &[String],
        capture: bool,
        extra_env: Option<&HashMap<String, String>>,
    ) -> Result<CmdResult> {
        self.run_with_timeout(cmd, capture, extra_env, None, None)
    }

    pub fn run_with_timeout(
        &self,
        cmd: &[String],
        capture: bool,
        extra_env: Option<&HashMap<String, String>>,
        timeout: Option<Duration>,
        heartbeat: Option<Duration>,
    ) -> Result<CmdResult> {
        if cmd.is_empty() {
            anyhow::bail!("empty command");
        }
        debug!(command = %cmd.join(" "), "[cmd]");
        let mut base_env = base_env_with_extra(extra_env);

        if cmd.get(0).map(|s| s == "fetch-ebook-metadata").unwrap_or(false)
            && self.headless_fetch
        {
            for (k, v) in &self.headless_env {
                base_env.entry(k.clone()).or_insert_with(|| v.clone());
            }
            debug!(headless = true, "[fetch-ebook-metadata] using headless Qt/WebEngine env");
        }

        let run_with_env = |env: &HashMap<String, String>| -> Result<CmdResult> {
            let mut command = Command::new(&cmd[0]);
            for arg in &cmd[1..] {
                command.arg(arg);
            }
            if capture {
                command.stdout(Stdio::piped()).stderr(Stdio::piped());
            }
            command.env_clear();
            for (k, v) in env {
                command.env(k, v);
            }
            if let Some(limit) = timeout {
                let mut child = command.spawn().with_context(|| {
                    format!("Failed to run command: {}", cmd.join(" "))
                })?;
                let tick = heartbeat.unwrap_or(Duration::from_secs(0));
                let start = Instant::now();
                let mut last_beat = Instant::now();
                loop {
                    let wait_dur = if tick.as_secs() == 0 { limit } else { Duration::from_secs(1) };
                    match child.wait_timeout(wait_dur)? {
                        Some(_) => {
                            let output = child.wait_with_output()?;
                            return Ok(CmdResult {
                                status_code: output.status.code().unwrap_or(1),
                                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                                timed_out: false,
                            });
                        }
                        None => {
                            if start.elapsed() >= limit {
                                let _ = child.kill();
                                let output = child.wait_with_output()?;
                                return Ok(CmdResult {
                                    status_code: 124,
                                    stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                                    stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                                    timed_out: true,
                                });
                            }
                            if tick.as_secs() > 0 && last_beat.elapsed() >= tick {
                                info!(elapsed_seconds = start.elapsed().as_secs(), "[fetch] still running...");
                                last_beat = Instant::now();
                            }
                        }
                    }
                }
            }

            let output = command.output().with_context(|| {
                format!("Failed to run command: {}", cmd.join(" "))
            })?;
            Ok(CmdResult {
                status_code: output.status.code().unwrap_or(1),
                stdout: String::from_utf8_lossy(&output.stdout).to_string(),
                stderr: String::from_utf8_lossy(&output.stderr).to_string(),
                timed_out: false,
            })
        };

        if is_calibredb(&cmd[0]) {
            if self.debug_calibredb_env {
                let keys = [
                    "PYTHONPATH",
                    "PYTHONHOME",
                    "PYTHONNOUSERSITE",
                    "PYTHONUSERBASE",
                    "VIRTUAL_ENV",
                    "UV_PROJECT_ENVIRONMENT",
                    "UV_PYTHON",
                    "UV_PYTHON_BIN",
                    "UV_SYSTEM_PYTHON",
                    "CONDA_PREFIX",
                    "POETRY_ACTIVE",
                    "PYENV_VERSION",
                    "PATH",
                ];
                debug!(
                    current_exe = %std::env::current_exe()
                        .ok()
                        .and_then(|p| p.to_str().map(|s| s.to_string()))
                        .unwrap_or_else(|| "<unknown>".to_string()),
                    "[calibredb debug]"
                );
                for k in keys {
                    if let Some(val) = base_env.get(k) {
                        debug!(key = %k, value = %val, "[calibredb debug]");
                    }
                }
            }

            match self.calibredb_env_mode {
                CalibreEnvMode::Clean => {
                    base_env.retain(|k, _| !should_clean_env_key(k));
                    return run_with_env(&base_env);
                }
                CalibreEnvMode::Override => {
                    let first = run_with_env(&base_env)?;
                    if first.status_code == 0 {
                        return Ok(first);
                    }
                    let mut last = first;
                    for overrides in CALIBRE_ENVS {
                        let mut env = base_env.clone();
                        for (k, v) in *overrides {
                            env.insert((*k).to_string(), (*v).to_string());
                        }
                        let attempt = run_with_env(&env)?;
                        last = attempt;
                        if last.status_code == 0 {
                            return Ok(last);
                        }
                    }
                    if !last.stderr.trim().is_empty() {
                        warn!(
                            stderr = %truncate(&trim_if_present(&last.stderr), 2000),
                            "[calibredb stderr]"
                        );
                    }
                    if !last.stdout.trim().is_empty() {
                        warn!(
                            stdout = %truncate(&trim_if_present(&last.stdout), 2000),
                            "[calibredb stdout]"
                        );
                    }
                    return Ok(last);
                }
                CalibreEnvMode::Inherit => {
                    let first = run_with_env(&base_env)?;
                    if first.status_code == 0 {
                        return Ok(first);
                    }
                    if !first.stderr.trim().is_empty() {
                        warn!(
                            stderr = %truncate(&trim_if_present(&first.stderr), 2000),
                            "[calibredb stderr]"
                        );
                    }
                    if !first.stdout.trim().is_empty() {
                        warn!(
                            stdout = %truncate(&trim_if_present(&first.stdout), 2000),
                            "[calibredb stdout]"
                        );
                    }
                    if first.stderr.contains("No module named 'msgpack'") {
                        base_env.retain(|k, _| !should_clean_env_key(k));
                        let retry = run_with_env(&base_env)?;
                        if retry.status_code == 0 {
                            info!("[calibredb] succeeded after cleaning env vars");
                            return Ok(retry);
                        }
                        if !retry.stderr.trim().is_empty() {
                            warn!(
                                stderr = %truncate(&trim_if_present(&retry.stderr), 2000),
                                "[calibredb retry stderr]"
                            );
                        }
                        return Ok(retry);
                    }
                    return Ok(first);
                }
            }
        }

        run_with_env(&base_env)
    }

    pub fn run_fetch_streaming(
        &self,
        cmd: &[String],
        timeout: Duration,
        heartbeat: Duration,
    ) -> Result<CmdResult> {
        if cmd.is_empty() {
            anyhow::bail!("empty command");
        }
        debug!(command = %cmd.join(" "), "[cmd]");
        let mut env = base_env_with_extra(None);
        if self.headless_fetch {
            for (k, v) in &self.headless_env {
                env.entry(k.clone()).or_insert_with(|| v.clone());
            }
            debug!(headless = true, "[fetch-ebook-metadata] using headless Qt/WebEngine env");
        }

        let mut command = if self.fetch_use_xvfb {
            info!("[fetch] using xvfb-run");
            let mut c = Command::new("xvfb-run");
            c.arg("-a");
            c.arg(&cmd[0]);
            for arg in &cmd[1..] {
                c.arg(arg);
            }
            c
        } else {
            let mut c = Command::new(&cmd[0]);
            for arg in &cmd[1..] {
                c.arg(arg);
            }
            c
        };
        command.stdout(Stdio::piped()).stderr(Stdio::piped());
        command.env_clear();
        for (k, v) in env {
            command.env(k, v);
        }

        let mut child = command.spawn().with_context(|| {
            format!("Failed to run command: {}", cmd.join(" "))
        })?;
        let stdout = child.stdout.take().ok_or_else(|| anyhow::anyhow!("missing stdout"))?;
        let stderr = child.stderr.take().ok_or_else(|| anyhow::anyhow!("missing stderr"))?;

        let (tx, rx) = mpsc::channel::<(bool, String)>();
        let tx_out = tx.clone();
        let tx_err = tx.clone();

        let out_handle = thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines().flatten() {
                let _ = tx_out.send((true, line));
            }
        });

        let err_handle = thread::spawn(move || {
            let reader = BufReader::new(stderr);
            for line in reader.lines().flatten() {
                let _ = tx_err.send((false, line));
            }
        });

        let start = Instant::now();
        let mut last_beat = Instant::now();
        let mut stdout_acc = String::new();
        let mut stderr_acc = String::new();

        loop {
            match child.wait_timeout(Duration::from_secs(1))? {
                Some(status) => {
                    for msg in rx.try_iter() {
                        if msg.0 {
                            info!("[fetch stdout] {}", msg.1);
                            stdout_acc.push_str(&msg.1);
                            stdout_acc.push('\n');
                        } else {
                            warn!("[fetch stderr] {}", msg.1);
                            stderr_acc.push_str(&msg.1);
                            stderr_acc.push('\n');
                        }
                    }
                    let _ = out_handle.join();
                    let _ = err_handle.join();
                    return Ok(CmdResult {
                        status_code: status.code().unwrap_or(1),
                        stdout: stdout_acc,
                        stderr: stderr_acc,
                        timed_out: false,
                    });
                }
                None => {
                    let mut received = false;
                    loop {
                        match rx.recv_timeout(Duration::from_millis(50)) {
                            Ok((is_out, line)) => {
                                received = true;
                                if is_out {
                                    info!("[fetch stdout] {}", line);
                                    stdout_acc.push_str(&line);
                                    stdout_acc.push('\n');
                                } else {
                                    warn!("[fetch stderr] {}", line);
                                    stderr_acc.push_str(&line);
                                    stderr_acc.push('\n');
                                }
                            }
                            Err(RecvTimeoutError::Timeout) => break,
                            Err(RecvTimeoutError::Disconnected) => break,
                        }
                    }

                    if start.elapsed() >= timeout {
                        let _ = child.kill();
                        let _ = child.wait();
                        let _ = out_handle.join();
                        let _ = err_handle.join();
                        return Ok(CmdResult {
                            status_code: 124,
                            stdout: stdout_acc,
                            stderr: stderr_acc,
                            timed_out: true,
                        });
                    }

                    if !received && heartbeat.as_secs() > 0 && last_beat.elapsed() >= heartbeat {
                        info!(elapsed_seconds = start.elapsed().as_secs(), "[fetch] still running...");
                        last_beat = Instant::now();
                    }
                }
            }
        }
    }
}
