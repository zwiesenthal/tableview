//! The feedback loop: the user describes a change, Claude Code makes it in its
//! own git worktree of TableView's source and TableView opens a pull request.
//! When the PR is merged on GitHub, TableView pulls `main`, rebuilds itself and
//! offers a restart into the new version.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use std::time::SystemTime;

use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;

use crate::chat::{Image, claude_cmd, path_env, user_message};
use crate::state::AppState;

const LOG_LINES: usize = 60;
const POLL_EVERY: Duration = Duration::from_secs(60);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Claude is making the change.
    Working,
    /// A pull request is open and waiting for review.
    PrOpen,
    /// The PR was merged; TableView is pulling and rebuilding.
    Merged,
    /// The new build is ready; restarting runs it.
    Ready,
    /// The running app already includes it.
    Installed,
    /// The PR was closed without merging.
    Closed,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub id: String,
    pub text: String,
    pub created_at: String,
    pub status: Status,
    pub branch: String,
    #[serde(default)]
    pub pr_url: Option<String>,
    #[serde(default)]
    pub summary: Option<String>,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub log: VecDeque<String>,
    /// Set when this request redoes an earlier one whose PR no longer applied to main.
    #[serde(default)]
    pub redo_of: Option<String>,
    /// How many pictures came with the request (stored beside the list).
    #[serde(default)]
    pub images: usize,
}

#[derive(Default)]
pub struct Feedback {
    items: Mutex<Vec<Item>>,
    /// One rebuild at a time.
    building: tokio::sync::Mutex<()>,
    loaded: std::sync::Once,
}

#[derive(Serialize)]
pub struct Readiness {
    pub source_dir: String,
    pub is_git: bool,
    pub has_github_remote: bool,
    pub gh_signed_in: bool,
    pub claude_found: bool,
    pub problems: Vec<String>,
    /// `owner/name` of origin when the signed-in GitHub user can't push to it:
    /// pull requests can't be opened there, so the panel offers a fork instead.
    pub fork_from: Option<String>,
}

/// `owner/name` from a GitHub remote URL (https, ssh or scp-style).
pub fn github_slug(url: &str) -> Option<String> {
    let rest = url.trim().split("github.com").nth(1)?;
    let rest = rest.trim_start_matches([':', '/']).trim_end_matches('/');
    let rest = rest.strip_suffix(".git").unwrap_or(rest);
    let mut parts = rest.split('/');
    let (owner, name) = (parts.next()?, parts.next()?);
    if owner.is_empty() || name.is_empty() || parts.next().is_some() {
        return None;
    }
    Some(format!("{owner}/{name}"))
}

/// Whether a GitHub `viewerPermission` lets the user push branches.
fn can_push(permission: &str) -> bool {
    matches!(permission, "ADMIN" | "MAINTAIN" | "WRITE")
}

/// The source checkout: the configured folder, else the one this binary was built from.
pub fn source_dir(state: &AppState) -> PathBuf {
    let configured = state.settings.lock().unwrap().updates.source_dir.clone();
    if !configured.trim().is_empty() {
        return PathBuf::from(crate::db::shellexpand_home(configured.trim()));
    }
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_default()
}

fn cmd(program: &str, cwd: &Path) -> Command {
    let mut c = Command::new(program);
    c.current_dir(cwd)
        .env("PATH", path_env())
        .stdin(Stdio::null())
        .kill_on_drop(true);
    c
}

/// Runs a command and returns trimmed stdout, or an error carrying stderr.
async fn run(program: &str, cwd: &Path, args: &[&str]) -> anyhow::Result<String> {
    let out = cmd(program, cwd)
        .args(args)
        .output()
        .await
        .map_err(|e| anyhow::anyhow!("could not run {program}: {e}"))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
        let msg = if err.is_empty() {
            String::from_utf8_lossy(&out.stdout).trim().to_string()
        } else {
            err
        };
        anyhow::bail!(
            "{program} {} failed: {msg}",
            args.first().copied().unwrap_or_default()
        );
    }
    Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
}

fn slug(text: &str) -> String {
    let s: String = text
        .to_lowercase()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .split('-')
        .filter(|p| !p.is_empty())
        .take(5)
        .collect::<Vec<_>>()
        .join("-");
    if s.is_empty() {
        "change".into()
    } else {
        s.chars().take(40).collect()
    }
}

fn title(text: &str) -> String {
    let first = text
        .lines()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .unwrap_or("Feedback");
    let mut t: String = first.chars().take(72).collect();
    if first.chars().count() > 72 {
        t.push('…');
    }
    t
}

impl Feedback {
    fn path(state: &AppState) -> PathBuf {
        state.store.dir().join("feedback.json")
    }

    fn load(&self, state: &AppState) {
        self.loaded.call_once(|| {
            if let Ok(bytes) = std::fs::read(Self::path(state))
                && let Ok(items) = serde_json::from_slice::<Vec<Item>>(&bytes)
            {
                *self.items.lock().unwrap() = items;
            }
            if running_latest(state) {
                let mut items = self.items.lock().unwrap();
                let mut changed = false;
                for i in items.iter_mut().filter(|i| i.status == Status::Ready) {
                    i.status = Status::Installed;
                    i.error = None;
                    changed = true;
                }
                drop(items);
                if changed {
                    self.save(state);
                }
            }
        });
    }

    fn save(&self, state: &AppState) {
        let items = self.items.lock().unwrap().clone();
        if let Ok(bytes) = serde_json::to_vec_pretty(&items) {
            let _ = crate::config::Store::write_private(&Self::path(state), &bytes);
        }
    }

    pub fn list(&self, state: &AppState) -> Vec<Item> {
        self.load(state);
        self.items.lock().unwrap().clone()
    }

    fn update(&self, state: &AppState, id: &str, f: impl FnOnce(&mut Item)) {
        let snapshot = {
            let mut items = self.items.lock().unwrap();
            let Some(item) = items.iter_mut().find(|i| i.id == id) else {
                return;
            };
            f(item);
            item.clone()
        };
        self.save(state);
        state.emit("feedback-updated", snapshot);
    }

    fn log(&self, state: &AppState, id: &str, line: impl Into<String>) {
        let line = line.into();
        self.update(state, id, |i| {
            i.log.push_back(line);
            while i.log.len() > LOG_LINES {
                i.log.pop_front();
            }
        });
    }

    pub async fn readiness(&self, state: &AppState) -> Readiness {
        let dir = source_dir(state);
        let mut problems = vec![];
        let is_git = run("git", &dir, &["rev-parse", "--is-inside-work-tree"])
            .await
            .is_ok();
        if !is_git {
            problems.push(format!(
                "{} is not a git checkout of TableView. Set the source folder in Settings.",
                dir.display()
            ));
        }
        let remote = run("git", &dir, &["remote", "get-url", "origin"])
            .await
            .unwrap_or_default();
        let has_github_remote = remote.contains("github.com");
        if is_git && !has_github_remote {
            problems.push(
                "The checkout has no GitHub remote named origin, so pull requests can't be opened."
                    .into(),
            );
        }
        let gh_signed_in = run("gh", &dir, &["auth", "status"]).await.is_ok();
        if !gh_signed_in {
            problems.push(
                "The GitHub CLI isn't installed or signed in. Run `gh auth login` in a terminal."
                    .into(),
            );
        }
        let claude_found = crate::chat::auth_status(state).await.logged_in;
        if !claude_found {
            problems.push("Claude Code isn't installed or signed in. Use the Sign in button in the Claude panel.".into());
        }
        // Without push access the branch push fails after Claude has done the work,
        // so ask GitHub up front. If GitHub can't be reached, don't block on it.
        let mut fork_from = None;
        if let (true, Some(slug)) = (gh_signed_in, github_slug(&remote)) {
            if let Ok(perm) = run("gh", &dir, &["repo", "view", &slug, "--json", "viewerPermission", "--jq", ".viewerPermission"]).await {
                if !can_push(perm.trim()) {
                    fork_from = Some(slug);
                }
            }
        }
        Readiness {
            source_dir: dir.display().to_string(),
            is_git,
            has_github_remote,
            gh_signed_in,
            claude_found,
            problems,
            fork_from,
        }
    }

    /// Forks origin to the signed-in user's GitHub account and makes the fork
    /// `origin` (the original becomes `upstream`), so feedback PRs open there.
    pub async fn fork(&self, state: &AppState) -> anyhow::Result<String> {
        let dir = source_dir(state);
        let remote = run("git", &dir, &["remote", "get-url", "origin"]).await?;
        let slug = github_slug(&remote)
            .ok_or_else(|| anyhow::anyhow!("origin isn't a GitHub repository: {remote}"))?;
        if run("git", &dir, &["remote", "get-url", "upstream"]).await.is_ok() {
            anyhow::bail!("This checkout already has a remote named upstream. Point origin at your fork with `git remote set-url origin …`.");
        }
        // No repository argument: gh only rewires remotes when forking the current checkout.
        run("gh", &dir, &["repo", "fork", "--remote", "--remote-name", "origin", "--default-branch-only"]).await?;
        let fork = run("git", &dir, &["remote", "get-url", "origin"]).await?;
        let fork = github_slug(&fork).unwrap_or(fork);
        if fork == slug {
            anyhow::bail!("GitHub made the fork, but origin still points at {slug}. Set origin to your fork with `git remote set-url origin …`.");
        }
        let _ = run("gh", &dir, &["repo", "set-default", &fork]).await;
        run("git", &dir, &["fetch", "--quiet", "origin"]).await?;
        Ok(fork)
    }

    /// Starts Claude on a change in a fresh worktree; returns the new item.
    pub async fn submit(
        self: &Arc<Self>,
        state: Arc<AppState>,
        text: String,
        images: Vec<Image>,
        redo_of: Option<String>,
    ) -> anyhow::Result<Item> {
        self.load(&state);
        let text = text.trim().to_string();
        user_message(&text, &images)?; // validate before starting anything
        if text.is_empty() {
            anyhow::bail!("Describe the change you want.");
        }
        let ready = self.readiness(&state).await;
        if let Some(p) = ready.problems.first() {
            anyhow::bail!("{p}");
        }
        if let Some(slug) = &ready.fork_from {
            anyhow::bail!("You can't push to {slug}. Fork it first, then TableView opens pull requests on your fork.");
        }
        let repo = source_dir(&state);
        let id = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
        let branch = format!("feedback/{}-{id}", slug(&text));
        let item = Item {
            id: id.clone(),
            text: text.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
            status: Status::Working,
            branch: branch.clone(),
            pr_url: None,
            summary: None,
            error: None,
            log: VecDeque::new(),
            redo_of,
            images: images.len(),
        };
        if !images.is_empty() {
            let dir = attachments_dir(&state);
            std::fs::create_dir_all(&dir)?;
            crate::config::Store::write_private(
                &dir.join(format!("{id}.json")),
                &serde_json::to_vec(&images)?,
            )?;
        }
        self.items.lock().unwrap().insert(0, item.clone());
        self.save(&state);

        let me = self.clone();
        tokio::spawn(async move {
            match me.work(&state, &repo, &id, &branch, &text, &images).await {
                Ok(true) => me.redo(state.clone(), &id).await,
                Ok(false) => {
                    let _ = me.check(state.clone()).await;
                }
                Err(e) => me.update(&state, &id, |i| {
                    i.status = Status::Failed;
                    i.error = Some(e.to_string());
                }),
            }
        });
        Ok(item)
    }

    async fn work(
        &self,
        state: &AppState,
        repo: &Path,
        id: &str,
        branch: &str,
        text: &str,
        images: &[Image],
    ) -> anyhow::Result<bool> {
        let worktrees = state.store.dir().join("feedback-worktrees");
        std::fs::create_dir_all(&worktrees)?;
        let wt = worktrees.join(id);
        self.log(state, id, "Fetching the latest main…");
        run("git", repo, &["fetch", "--quiet", "origin", "main"]).await?;
        run(
            "git",
            repo,
            &[
                "worktree",
                "add",
                "-q",
                "-b",
                branch,
                &wt.to_string_lossy(),
                "origin/main",
            ],
        )
        .await?;
        if repo.join("node_modules").exists() && !wt.join("node_modules").exists() {
            #[cfg(unix)]
            let _ = std::os::unix::fs::symlink(repo.join("node_modules"), wt.join("node_modules"));
        }

        let pics = if images.is_empty() {
            String::new()
        } else {
            "The pictures above came with the feedback (usually screenshots of TableView); use them to see what the user means.\n".into()
        };
        let prompt = format!(
            "You are improving TableView, the macOS database client in this repository, from in-app feedback.\n\n\
             Feedback from the user:\n<feedback>\n{text}\n</feedback>\n{pics}\n\
             Make the smallest complete change that does what the feedback asks, matching the code's existing style. \
             Read CONTRIBUTING.md and docs/architecture.md first if the change is not trivial. \
             Then verify: `npx tsc --noEmit`, `npx vite build`, and `cd src-tauri && cargo test` must all pass. \
             Commit your work on the current branch with a clear message. Do not push and do not open a pull request; TableView does that. \
             Finish with a short summary of what you changed and how you checked it."
        );
        let allowed = [
            "Read",
            "Edit",
            "Write",
            "Glob",
            "Grep",
            "Bash(npm run *)",
            "Bash(npm install*)",
            "Bash(npx *)",
            "Bash(cargo *)",
            "Bash(cd *)",
            "Bash(git status*)",
            "Bash(git diff*)",
            "Bash(git log*)",
            "Bash(git add *)",
            "Bash(git commit *)",
            "Bash(ls *)",
            "Bash(cat *)",
            "Bash(rg *)",
            "Bash(grep *)",
        ]
        .join(",");
        let mut claude = claude_cmd(state)?;
        claude
            .args([
                "-p",
                "--input-format",
                "stream-json",
                "--output-format",
                "stream-json",
                "--verbose",
                "--permission-mode",
                "acceptEdits",
            ])
            .arg("--allowedTools")
            .arg(allowed)
            .arg("--disallowedTools")
            .arg("Bash(git push*),Bash(gh *),WebFetch,WebSearch")
            .current_dir(&wt)
            .env("CARGO_TARGET_DIR", repo.join("src-tauri").join("target"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        self.log(state, id, "Claude is working on it…");
        let mut child = claude
            .spawn()
            .map_err(|e| anyhow::anyhow!("could not start claude: {e}"))?;
        {
            use tokio::io::AsyncWriteExt;
            let mut stdin = child.stdin.take().unwrap();
            let msg = user_message(&prompt, images)?;
            stdin.write_all(format!("{msg}\n").as_bytes()).await?;
            stdin.shutdown().await?; // one message, then end of input
        }
        let stdout = child.stdout.take().unwrap();
        let mut lines = BufReader::new(stdout).lines();
        let mut summary = None;
        while let Ok(Some(line)) = lines.next_line().await {
            let Ok(ev) = serde_json::from_str::<serde_json::Value>(&line) else {
                continue;
            };
            match ev["type"].as_str() {
                Some("assistant") => {
                    for c in ev["message"]["content"].as_array().into_iter().flatten() {
                        if c["type"] == "tool_use" {
                            let name = c["name"].as_str().unwrap_or("tool");
                            let detail = c["input"]["command"]
                                .as_str()
                                .or_else(|| c["input"]["file_path"].as_str())
                                .unwrap_or("");
                            self.log(
                                state,
                                id,
                                format!("{name} {}", detail.chars().take(120).collect::<String>()),
                            );
                        } else if let Some(t) = c["text"].as_str()
                            && let Some(first) = t.lines().find(|l| !l.trim().is_empty())
                        {
                            self.log(state, id, first.chars().take(160).collect::<String>());
                        }
                    }
                }
                Some("result") => {
                    if ev["is_error"].as_bool() == Some(true) {
                        anyhow::bail!(
                            "Claude stopped: {}",
                            ev["result"].as_str().unwrap_or("unknown error")
                        );
                    }
                    summary = ev["result"].as_str().map(str::to_string);
                }
                _ => {}
            }
        }
        let status = child.wait().await?;
        if !status.success() && summary.is_none() {
            anyhow::bail!("Claude exited with {status}");
        }

        if !run("git", &wt, &["status", "--porcelain"])
            .await?
            .is_empty()
        {
            run("git", &wt, &["add", "-A"]).await?;
            run(
                "git",
                &wt,
                &["commit", "-q", "-m", &format!("Feedback: {}", title(text))],
            )
            .await?;
        }
        if run("git", &wt, &["log", "--oneline", "origin/main..HEAD"])
            .await?
            .is_empty()
        {
            anyhow::bail!(
                "Claude finished without changing anything. {}",
                summary.unwrap_or_default()
            );
        }

        self.log(state, id, "Pushing the branch and opening a pull request…");
        run("git", &wt, &["push", "-q", "-u", "origin", branch]).await?;
        let body = format!(
            "**Feedback**\n\n> {}\n\n**What Claude changed**\n\n{}\n\n---\nOpened from TableView's feedback panel. Merging this updates TableView the next time it checks.",
            text.replace('\n', "\n> "),
            summary.clone().unwrap_or_else(|| "See the commits.".into()),
        );
        // Open the PR on origin itself. After a fork, gh would otherwise pick the
        // `upstream` remote and send the PR to a repo this user can't merge in.
        let origin = run("git", &wt, &["remote", "get-url", "origin"]).await?;
        let target = github_slug(&origin)
            .ok_or_else(|| anyhow::anyhow!("origin isn't a GitHub repository: {origin}"))?;
        let url = run(
            "gh",
            &wt,
            &[
                "pr",
                "create",
                "--repo",
                &target,
                "--base",
                "main",
                "--head",
                branch,
                "--title",
                &title(text),
                "--body",
                &body,
            ],
        )
        .await?;
        let url = url
            .lines()
            .rev()
            .find(|l| l.starts_with("https://"))
            .unwrap_or(&url)
            .to_string();
        self.update(state, id, |i| {
            i.status = Status::PrOpen;
            i.pr_url = Some(url.clone());
            i.summary = summary;
        });
        self.log(state, id, format!("Pull request opened: {url}"));

        Ok(self.try_merge(state, repo, id, &url).await)
    }

    /// Boxed so the submit → redo → submit cycle has a nameable, `Send` future type.
    fn resubmit(
        me: Arc<Self>,
        state: Arc<AppState>,
        text: String,
        redo_of: String,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = anyhow::Result<Item>> + Send>> {
        Box::pin(async move {
            let images = load_images(&state, &redo_of);
            me.submit(state, text, images, Some(redo_of)).await
        })
    }

    /// Closes a PR that no longer applies to main and asks Claude for the same change again.
    async fn redo(self: &Arc<Self>, state: Arc<AppState>, id: &str) {
        let Some(item) = self.list(&state).into_iter().find(|i| i.id == id) else {
            return;
        };
        let repo = source_dir(&state);
        if let Some(url) = &item.pr_url {
            let _ = run("gh", &repo, &["pr", "close", url, "--delete-branch", "--comment",
                "Main moved on and this no longer applied, so TableView asked Claude to redo it on the current main."]).await;
        }
        let wt = state.store.dir().join("feedback-worktrees").join(id);
        let _ = run(
            "git",
            &repo,
            &["worktree", "remove", "--force", &wt.to_string_lossy()],
        )
        .await;
        self.update(&state, id, |i| {
            i.status = Status::Closed;
            i.error = Some(
                "It no longer applied to the current main, so it was redone as a new request."
                    .into(),
            );
        });
        if let Err(e) = Self::resubmit(
            self.clone(),
            state.clone(),
            item.text.clone(),
            item.id.clone(),
        )
        .await
        {
            self.update(&state, id, |i| {
                i.error = Some(format!("Redo failed to start: {e}"))
            });
        }
    }

    /// With automatic merging on: rebase the PR onto the current main, run
    /// TableView's own checks, and merge if they pass. Any problem leaves the
    /// PR open with the reason on the item, and it is not retried until edited.
    async fn try_merge(&self, state: &AppState, repo: &Path, id: &str, url: &str) -> bool {
        if !state.settings.lock().unwrap().updates.auto_merge {
            return false;
        }
        let is_redo = self
            .list(state)
            .iter()
            .any(|i| i.id == id && i.redo_of.is_some());
        let mut conflicted = false;
        let wt = state.store.dir().join("feedback-worktrees").join(id);
        let result: Result<(), String> = async {
            if !wt.exists() {
                return Err("Its working folder is gone, so TableView can't check it. Merge it on GitHub if it looks right.".into());
            }
            let branch = run("git", &wt, &["rev-parse", "--abbrev-ref", "HEAD"]).await.map_err(|e| e.to_string())?;
            run("git", &wt, &["fetch", "--quiet", "origin", "main"]).await.map_err(|e| e.to_string())?;
            let behind = run("git", &wt, &["rev-list", "--count", "HEAD..origin/main"]).await.map_err(|e| e.to_string())?;
            if behind != "0" {
                self.log(state, id, "Main has moved on; rebasing onto it…");
                if let Err(e) = run("git", &wt, &["rebase", "origin/main"]).await {
                    let _ = run("git", &wt, &["rebase", "--abort"]).await;
                    conflicted = true;
                    return Err(format!("It conflicts with the current main, so it needs a manual merge.\n{e}"));
                }
                run("git", &wt, &["push", "-q", "--force-with-lease", "origin", &branch]).await.map_err(|e| e.to_string())?;
            }
            self.log(state, id, "Checking the change before merging (type-check, build, tests)…");
            verify(&wt, repo).await?;
            self.log(state, id, "Checks passed. Merging…");
            // `--delete-branch` would also check out main locally, which fails in a worktree.
            run("gh", &wt, &["pr", "merge", url, "--squash"]).await.map_err(|e| e.to_string())?;
            let _ = run("git", &wt, &["push", "-q", "origin", "--delete", &branch]).await;
            self.log(state, id, "Merged.");
            Ok(())
        }
        .await;
        match result {
            Ok(()) => false,
            Err(_) if conflicted && !is_redo => true,
            Err(reason) => {
                self.update(state, id, |i| {
                    i.error = Some(format!(
                        "Not merged automatically; the pull request is open for review.\n\n{reason}"
                    ));
                });
                false
            }
        }
    }

    /// Checks open PRs; a merged one triggers the update. Returns how many merged.
    pub async fn check(self: &Arc<Self>, state: Arc<AppState>) -> anyhow::Result<usize> {
        self.load(&state);
        let open: Vec<Item> = self
            .items
            .lock()
            .unwrap()
            .iter()
            .filter(|i| i.status == Status::PrOpen)
            .cloned()
            .collect();
        let repo = source_dir(&state);
        let mut merged = 0;
        for item in &open {
            if let (Some(url), None) = (&item.pr_url, &item.error) {
                if self.try_merge(&state, &repo, &item.id, url).await {
                    self.redo(state.clone(), &item.id).await;
                }
            }
        }
        for item in open {
            let Some(url) = &item.pr_url else { continue };
            let Ok(out) = run("gh", &repo, &["pr", "view", url, "--json", "state"]).await else {
                continue;
            };
            let pr_state = serde_json::from_str::<serde_json::Value>(&out)
                .ok()
                .and_then(|v| v["state"].as_str().map(str::to_string))
                .unwrap_or_default();
            match pr_state.as_str() {
                "MERGED" => {
                    merged += 1;
                    self.update(&state, &item.id, |i| {
                        i.status = Status::Merged;
                        i.error = None;
                    });
                    let wt = state.store.dir().join("feedback-worktrees").join(&item.id);
                    let _ = run(
                        "git",
                        &repo,
                        &["worktree", "remove", "--force", &wt.to_string_lossy()],
                    )
                    .await;
                }
                "CLOSED" => {
                    self.update(&state, &item.id, |i| i.status = Status::Closed);
                    let wt = state.store.dir().join("feedback-worktrees").join(&item.id);
                    let _ = run(
                        "git",
                        &repo,
                        &["worktree", "remove", "--force", &wt.to_string_lossy()],
                    )
                    .await;
                }
                _ => {}
            }
        }
        // Also pick up merged changes whose update never got built (e.g. the app restarted mid-build).
        let unbuilt = self
            .list(&state)
            .iter()
            .any(|i| i.status == Status::Merged && i.error.is_none());
        if (merged > 0 || unbuilt) && self.building.try_lock().is_ok() {
            let me = self.clone();
            tokio::spawn(async move {
                let _ = me.rebuild(state).await;
            });
        }
        Ok(merged)
    }

    /// Pulls main into the source checkout and rebuilds the app bundle.
    pub async fn rebuild(self: &Arc<Self>, state: Arc<AppState>) -> anyhow::Result<()> {
        let _guard = self.building.lock().await;
        {
            let mut items = self.items.lock().unwrap();
            for i in items.iter_mut().filter(|i| i.status == Status::Merged) {
                i.error = None;
            }
        }
        let repo = source_dir(&state);
        let progress = |line: &str| state.emit("update-progress", json!({ "line": line }));
        let result: anyhow::Result<()> = async {
            let branch = run("git", &repo, &["rev-parse", "--abbrev-ref", "HEAD"]).await?;
            if branch != "main" {
                anyhow::bail!(
                    "Your source checkout is on branch {branch}. Switch it to main to update."
                );
            }
            if !run(
                "git",
                &repo,
                &["status", "--porcelain", "--untracked-files=no"],
            )
            .await?
            .is_empty()
            {
                anyhow::bail!(
                    "Your source checkout has uncommitted changes. Commit or stash them to update."
                );
            }
            progress("Pulling main…");
            let lock_before = std::fs::read(repo.join("package-lock.json")).unwrap_or_default();
            run(
                "git",
                &repo,
                &["pull", "--quiet", "--ff-only", "origin", "main"],
            )
            .await?;
            if std::fs::read(repo.join("package-lock.json")).unwrap_or_default() != lock_before {
                progress("Installing updated npm packages…");
                run("npm", &repo, &["install", "--no-audit", "--no-fund"]).await?;
            }
            // Skip the build when the bundle on disk is already newer than main.
            let head_time = run("git", &repo, &["log", "-1", "--format=%ct"]).await?;
            let built_exe = bundles(&state).0.join("Contents/MacOS/tableview");
            let built_secs = std::fs::metadata(&built_exe)
                .and_then(|m| m.modified())
                .ok()
                .and_then(|t| t.duration_since(SystemTime::UNIX_EPOCH).ok())
                .map(|d| d.as_secs());
            if let (Some(b), Ok(h)) = (built_secs, head_time.trim().parse::<u64>())
                && b > h
                && run(
                    "git",
                    &repo,
                    &["status", "--porcelain", "--untracked-files=no"],
                )
                .await?
                .is_empty()
            {
                return Ok(());
            }
            progress("Building the new version (a few minutes)…");
            let identity = state
                .settings
                .lock()
                .unwrap()
                .updates
                .signing_identity
                .clone();
            let mut build = cmd("npm", &repo);
            build
                .args(["run", "tauri", "build", "--", "--bundles", "app"])
                .stdout(Stdio::piped())
                .stderr(Stdio::piped());
            // Ad-hoc signatures change with every build, and macOS then asks for the
            // login password before the new build may read saved passwords. A real
            // certificate keeps the app's identity, so "Always Allow" sticks.
            let identity = match identity.trim() {
                "" => default_signing_identity().await,
                id => Some(id.to_string()),
            };
            if let Some(identity) = identity {
                build.env("APPLE_SIGNING_IDENTITY", identity);
            }
            let mut child = build.spawn()?;
            let mut err = BufReader::new(child.stderr.take().unwrap()).lines();
            let mut tail = VecDeque::new();
            while let Ok(Some(line)) = err.next_line().await {
                if line.contains("Compiling")
                    || line.contains("Finished")
                    || line.contains("Bundling")
                {
                    progress(line.trim());
                }
                tail.push_back(line);
                if tail.len() > 20 {
                    tail.pop_front();
                }
            }
            if !child.wait().await?.success() {
                anyhow::bail!(
                    "The build failed:\n{}",
                    tail.into_iter().collect::<Vec<_>>().join("\n")
                );
            }
            Ok(())
        }
        .await;
        match &result {
            Ok(()) => {
                {
                    let mut items = self.items.lock().unwrap();
                    let installed = running_latest(&state);
                    for i in items.iter_mut().filter(|i| i.status == Status::Merged) {
                        i.status = if installed {
                            Status::Installed
                        } else {
                            Status::Ready
                        };
                        i.error = None;
                    }
                }
                self.save(&state);
                for i in self.list(&state) {
                    state.emit("feedback-updated", &i);
                }
                if !running_latest(&state) {
                    state.emit("update-ready", json!({}));
                }
            }
            Err(e) => {
                let msg = format!("Merged, but the update couldn't be built.\n\n{e}");
                {
                    let mut items = self.items.lock().unwrap();
                    for i in items.iter_mut().filter(|i| i.status == Status::Merged) {
                        i.error = Some(msg.clone());
                    }
                }
                self.save(&state);
                for i in self
                    .list(&state)
                    .into_iter()
                    .filter(|i| i.status == Status::Merged)
                {
                    state.emit("feedback-updated", &i);
                }
                state.emit("update-failed", json!({ "error": e.to_string() }));
            }
        }
        result
    }

    /// Checks for merges every minute while the app runs.
    pub fn start_polling(self: &Arc<Self>, state: Arc<AppState>) {
        let me = self.clone();
        tauri::async_runtime::spawn(async move {
            loop {
                tokio::time::sleep(POLL_EVERY).await;
                let has_open = me.list(&state).iter().any(|i| {
                    i.status == Status::PrOpen || (i.status == Status::Merged && i.error.is_none())
                });
                if has_open {
                    let _ = me.check(state.clone()).await;
                }
            }
        });
    }

    /// Clears a failed automatic merge so the next check tries again.
    pub async fn retry(self: &Arc<Self>, state: Arc<AppState>, id: &str) -> anyhow::Result<usize> {
        self.load(&state);
        self.update(&state, id, |i| {
            if i.status == Status::PrOpen {
                i.error = None;
            }
        });
        self.check(state).await
    }

    pub fn remove(&self, state: &AppState, id: &str) {
        self.load(state);
        self.items.lock().unwrap().retain(|i| i.id != id);
        self.save(state);
        let _ = std::fs::remove_file(attachments_dir(state).join(format!("{id}.json")));
    }
}

/// TableView's own gate before an automatic merge; Claude's word is not enough.
async fn verify(wt: &Path, repo: &Path) -> Result<(), String> {
    let target = repo.join("src-tauri").join("target");
    let steps: [(&str, &[&str], PathBuf); 3] = [
        ("npx", &["tsc", "--noEmit"], wt.to_path_buf()),
        ("npx", &["vite", "build"], wt.to_path_buf()),
        ("cargo", &["test", "-q"], wt.join("src-tauri")),
    ];
    for (program, args, dir) in steps {
        let out = cmd(program, &dir)
            .args(args)
            .env("CARGO_TARGET_DIR", &target)
            .output()
            .await
            .map_err(|e| format!("could not run {program}: {e}"))?;
        if !out.status.success() {
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            let tail: Vec<&str> = text
                .lines()
                .rev()
                .take(15)
                .collect::<Vec<_>>()
                .into_iter()
                .rev()
                .collect();
            return Err(format!(
                "`{program} {}` failed:\n{}",
                args.join(" "),
                tail.join("\n")
            ));
        }
    }
    Ok(())
}

/// The freshly built bundle, and the bundle this process runs from.
pub fn bundles(state: &AppState) -> (PathBuf, Option<PathBuf>) {
    let built = source_dir(state).join("src-tauri/target/release/bundle/macos/TableView.app");
    let running = std::env::current_exe().ok().and_then(|exe| {
        exe.ancestors()
            .find(|p| p.extension().is_some_and(|e| e == "app"))
            .map(Path::to_path_buf)
    });
    (built, running)
}

/// Puts the new build where the running app lives (if that's elsewhere) before a restart.
pub async fn install_build(state: &AppState) -> anyhow::Result<()> {
    let (built, running) = bundles(state);
    if !built.exists() {
        anyhow::bail!("No new build found at {}", built.display());
    }
    if let Some(running) = running
        && running != built
    {
        run(
            "ditto",
            Path::new("/"),
            &[&built.to_string_lossy(), &running.to_string_lossy()],
        )
        .await?;
    }
    Ok(())
}

#[cfg(test)]
mod slug_tests {
    use super::{can_push, github_slug, pick_identity};

    #[test]
    fn parses_github_remotes() {
        let want = Some("zwiesenthal/tableview".to_string());
        assert_eq!(github_slug("https://github.com/zwiesenthal/tableview.git"), want);
        assert_eq!(github_slug("https://github.com/zwiesenthal/tableview"), want);
        assert_eq!(github_slug("https://@github.com/zwiesenthal/tableview.git"), want);
        assert_eq!(github_slug("git@github.com:zwiesenthal/tableview.git"), want);
        assert_eq!(github_slug("ssh://git@github.com/zwiesenthal/tableview.git\n"), want);
        assert_eq!(github_slug("https://gitlab.com/a/b.git"), None);
        assert_eq!(github_slug("https://github.com/zwiesenthal"), None);
    }

    #[test]
    fn picks_a_stable_signing_identity() {
        let dev = "1111111111111111111111111111111111111111";
        let id = "2222222222222222222222222222222222222222";
        let listing = format!(
            "  1) {dev} \"Apple Development: a@b.c (TEAM1)\"\n  2) {id} \"Developer ID Application: Zed (TEAM2)\"\n     2 valid identities found"
        );
        assert_eq!(pick_identity(&listing).as_deref(), Some(id));
        let only_dev = format!("  1) {dev} \"Apple Development: a@b.c (TEAM1)\"\n");
        assert_eq!(pick_identity(&only_dev).as_deref(), Some(dev));
        assert_eq!(pick_identity("     0 valid identities found"), None);
    }

    #[test]
    fn push_permissions() {
        assert!(can_push("ADMIN") && can_push("MAINTAIN") && can_push("WRITE"));
        assert!(!can_push("READ") && !can_push("TRIAGE") && !can_push(""));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn branch_slugs_and_titles() {
        assert_eq!(
            slug("Make the sidebar WIDER, please!"),
            "make-the-sidebar-wider-please"
        );
        assert_eq!(slug("😀"), "change");
        assert_eq!(
            title("\n  Add dark mode toggle\nmore detail"),
            "Add dark mode toggle"
        );
        assert!(title(&"x".repeat(100)).ends_with('…'));
    }
}

fn attachments_dir(state: &AppState) -> PathBuf {
    state.store.dir().join("feedback-attachments")
}

fn load_images(state: &AppState, id: &str) -> Vec<Image> {
    std::fs::read(attachments_dir(state).join(format!("{id}.json")))
        .ok()
        .and_then(|b| serde_json::from_slice(&b).ok())
        .unwrap_or_default()
}

static STARTED: std::sync::OnceLock<SystemTime> = std::sync::OnceLock::new();

/// Records when this process started, to tell whether the latest build is the one running.
pub fn mark_start() {
    STARTED.get_or_init(SystemTime::now);
}

/// Whether the build on disk finished before this process started, i.e. it's the one running.
/// A code-signing identity in the login keychain to sign rebuilds with when
/// none is configured: Developer ID first, then Apple Development. Returns its
/// SHA-1, which stays unambiguous when two certificates share a name.
async fn default_signing_identity() -> Option<String> {
    if let Ok(id) = std::env::var("APPLE_SIGNING_IDENTITY")
        && !id.trim().is_empty()
    {
        return Some(id);
    }
    let out = run("security", Path::new("/"), &["find-identity", "-v", "-p", "codesigning"])
        .await
        .ok()?;
    pick_identity(&out)
}

fn pick_identity(listing: &str) -> Option<String> {
    let ids: Vec<(&str, &str)> = listing
        .lines()
        .filter_map(|l| {
            let mut parts = l.trim().splitn(3, ' ');
            let _index = parts.next()?;
            let sha = parts.next()?;
            let name = parts.next()?.trim().trim_matches('"');
            (sha.len() == 40 && sha.chars().all(|c| c.is_ascii_hexdigit())).then_some((sha, name))
        })
        .collect();
    ["Developer ID Application:", "Apple Development:"]
        .iter()
        .find_map(|prefix| ids.iter().find(|(_, n)| n.starts_with(prefix)))
        .map(|(sha, _)| sha.to_string())
}

fn running_latest(state: &AppState) -> bool {
    let (built, _) = bundles(state);
    std::fs::metadata(built.join("Contents/MacOS/tableview"))
        .and_then(|m| m.modified())
        .is_ok_and(|t| t <= *STARTED.get_or_init(SystemTime::now))
}
