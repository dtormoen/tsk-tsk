//! Asset management system for TSK
//!
//! This module provides template and dockerfile asset management. Dockerfiles
//! are served from embedded assets compiled into the binary. Templates are
//! discovered from project-level (`.tsk/templates/`), user-level
//! (`~/.config/tsk/templates/`), and embedded sources in priority order.

use anyhow::{Result, anyhow};
use std::path::Path;

pub mod embedded;
pub(crate) mod frontmatter;
pub mod utils;

use crate::context::tsk_env::TskEnv;

/// Check for deprecated filesystem-based dockerfile directories and warn the user.
///
/// Previously, TSK loaded custom dockerfiles from `.tsk/dockerfiles/` and
/// `~/.config/tsk/dockerfiles/`. These are now replaced by inline config fields
/// (`setup`, `stack_config`, `agent_config`) in `tsk.toml`.
pub fn warn_deprecated_dockerfiles(project_root: Option<&Path>, tsk_env: &TskEnv) {
    let paths: Vec<(&str, std::path::PathBuf)> = [
        (
            ".tsk/tsk.toml",
            project_root.map(|r| r.join(".tsk").join("dockerfiles")),
        ),
        (
            "~/.config/tsk/tsk.toml",
            Some(tsk_env.config_dir().join("dockerfiles")),
        ),
    ]
    .into_iter()
    .filter_map(|(config, path)| path.map(|p| (config, p)))
    .filter(|(_, path)| path.exists())
    .collect();

    for (config_location, path) in paths {
        let mut layers = Vec::new();
        if path.join("project").exists() {
            layers.push("  - project/*.dockerfile → `setup` field");
        }
        if path.join("stack").exists() {
            layers.push("  - stack/*.dockerfile   → `[stack_config.<name>]` setup field");
        }
        if path.join("agent").exists() {
            layers.push("  - agent/*.dockerfile   → `[agent_config.<name>]` setup field");
        }
        eprintln!(
            "\x1b[31mWarning: Found removed dockerfile directory: {}\x1b[0m\n\
             Filesystem-based Docker layers have been removed and are no longer loaded.\n\
             Migrate to inline config in {}:\n\
             {}\n\
             See the README for the new configuration format.",
            path.display(),
            config_location,
            layers.join("\n"),
        );
    }
}

/// Find a template by name, checking project, user, and embedded sources in priority order.
pub fn find_template(name: &str, project_root: Option<&Path>, tsk_env: &TskEnv) -> Result<String> {
    let filename = format!("{name}.md");

    // Check project level first
    if let Some(root) = project_root {
        let project_path = root.join(".tsk").join("templates").join(&filename);
        if project_path.exists() {
            return std::fs::read_to_string(&project_path).map_err(|e| {
                anyhow!(
                    "Failed to read template '{}': {}",
                    project_path.display(),
                    e
                )
            });
        }
    }

    // Check user level
    let user_path = tsk_env.config_dir().join("templates").join(&filename);
    if user_path.exists() {
        return std::fs::read_to_string(&user_path)
            .map_err(|e| anyhow!("Failed to read template '{}': {}", user_path.display(), e));
    }

    // Fall back to embedded
    embedded::get_template(name)
}

/// Find the on-disk path of a template, if it exists as a file.
/// Returns None for embedded-only templates.
pub fn find_template_path(
    name: &str,
    project_root: Option<&Path>,
    tsk_env: &TskEnv,
) -> Option<std::path::PathBuf> {
    let filename = format!("{name}.md");

    // Check project level first
    if let Some(root) = project_root {
        let project_path = root.join(".tsk").join("templates").join(&filename);
        if project_path.exists() {
            return Some(project_path);
        }
    }

    // Check user level
    let user_path = tsk_env.config_dir().join("templates").join(&filename);
    if user_path.exists() {
        return Some(user_path);
    }

    None
}

/// List all available templates from project, user, and embedded sources.
pub fn list_all_templates(project_root: Option<&Path>, tsk_env: &TskEnv) -> Vec<String> {
    let mut templates = std::collections::HashSet::new();

    // Check project level
    if let Some(root) = project_root {
        scan_template_dir(&root.join(".tsk").join("templates"), &mut templates);
    }

    // Check user level
    scan_template_dir(&tsk_env.config_dir().join("templates"), &mut templates);

    // Check embedded
    for name in embedded::list_templates() {
        templates.insert(name);
    }

    let mut result: Vec<String> = templates.into_iter().collect();
    result.sort();
    result
}

fn scan_template_dir(dir: &Path, templates: &mut std::collections::HashSet<String>) {
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Some(filename) = entry.file_name().to_str()
                && let Some(name) = filename.strip_suffix(".md")
            {
                templates.insert(name.to_string());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::AppContext;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_find_template_embedded_fallback() {
        let ctx = AppContext::builder().build();
        let result = find_template("feat", None, &ctx.tsk_env());
        assert!(result.is_ok());
        assert!(result.unwrap().contains("{{PROMPT}}"));
    }

    #[test]
    fn test_find_template_not_found() {
        let ctx = AppContext::builder().build();
        let result = find_template("nonexistent-xyz", None, &ctx.tsk_env());
        assert!(result.is_err());
    }

    #[test]
    fn test_find_template_project_priority() {
        let ctx = AppContext::builder().build();
        let temp_dir = TempDir::new().unwrap();
        let templates_dir = temp_dir.path().join(".tsk").join("templates");
        fs::create_dir_all(&templates_dir).unwrap();
        fs::write(templates_dir.join("feat.md"), "project-level feat").unwrap();

        let result = find_template("feat", Some(temp_dir.path()), &ctx.tsk_env());
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "project-level feat");
    }

    #[test]
    fn test_list_all_templates_includes_embedded() {
        let ctx = AppContext::builder().build();
        let templates = list_all_templates(None, &ctx.tsk_env());
        assert!(templates.contains(&"feat".to_string()));
        assert!(templates.contains(&"fix".to_string()));
        assert!(templates.contains(&"tsk-review".to_string()));
    }

    #[test]
    fn test_find_template_tsk_review() {
        let ctx = AppContext::builder().build();
        let result = find_template("tsk-review", None, &ctx.tsk_env());
        assert!(result.is_ok());
        assert!(result.unwrap().contains("{{PROMPT}}"));
    }

    #[test]
    fn test_list_all_templates_includes_project_and_deduplicates() {
        let ctx = AppContext::builder().build();
        let temp_dir = TempDir::new().unwrap();
        let templates_dir = temp_dir.path().join(".tsk").join("templates");
        fs::create_dir_all(&templates_dir).unwrap();
        fs::write(templates_dir.join("feat.md"), "override").unwrap();
        fs::write(templates_dir.join("custom-task.md"), "custom").unwrap();

        let templates = list_all_templates(Some(temp_dir.path()), &ctx.tsk_env());
        // Should include both embedded and project templates
        assert!(templates.contains(&"feat".to_string()));
        assert!(templates.contains(&"custom-task".to_string()));
        // Should be deduplicated (feat appears once, not twice)
        assert_eq!(
            templates.iter().filter(|t| *t == "feat").count(),
            1,
            "feat should appear exactly once"
        );
        // Should be sorted
        let mut sorted = templates.clone();
        sorted.sort();
        assert_eq!(templates, sorted);
    }

    #[test]
    fn test_warn_deprecated_dockerfiles_no_warning_when_absent() {
        let ctx = AppContext::builder().build();
        let temp_dir = TempDir::new().unwrap();
        // No .tsk/dockerfiles/ directory exists — should not panic or error
        warn_deprecated_dockerfiles(Some(temp_dir.path()), &ctx.tsk_env());
    }

    #[test]
    fn test_warn_deprecated_dockerfiles_detects_project_dir() {
        let ctx = AppContext::builder().build();
        let temp_dir = TempDir::new().unwrap();
        let dockerfiles_dir = temp_dir.path().join(".tsk").join("dockerfiles");
        fs::create_dir_all(dockerfiles_dir.join("project")).unwrap();
        fs::create_dir_all(dockerfiles_dir.join("stack")).unwrap();
        // Function runs without error when deprecated dirs exist
        // (warning is printed to stderr)
        warn_deprecated_dockerfiles(Some(temp_dir.path()), &ctx.tsk_env());
    }

    #[test]
    fn test_find_template_path_returns_none_for_embedded() {
        let ctx = AppContext::builder().build();
        // "feat" exists as embedded but not on disk in a fresh test env
        let result = find_template_path("feat", None, &ctx.tsk_env());
        assert!(result.is_none());
    }

    #[test]
    fn test_find_template_path_returns_path_for_project() {
        let ctx = AppContext::builder().build();
        let temp_dir = TempDir::new().unwrap();
        let templates_dir = temp_dir.path().join(".tsk").join("templates");
        fs::create_dir_all(&templates_dir).unwrap();
        fs::write(templates_dir.join("feat.md"), "project feat").unwrap();

        let result = find_template_path("feat", Some(temp_dir.path()), &ctx.tsk_env());
        assert!(result.is_some());
        assert_eq!(result.unwrap(), templates_dir.join("feat.md"));
    }
}
