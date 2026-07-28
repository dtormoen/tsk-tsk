//! Docker layer composition engine
//!
//! This module handles the composition of multiple Docker layers into a single
//! Dockerfile using template rendering.

use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};

use crate::context::tsk_config::ResolvedConfig;
use crate::docker::layers::DockerImageConfig;
use crate::docker::template_engine::DockerTemplateEngine;

/// Inline layer content from TOML config that overrides embedded asset lookups.
#[derive(Debug, Default)]
pub struct InlineLayerOverrides {
    /// Content for the stack layer position (from `stack_config[stack].setup`)
    pub stack_setup: Option<String>,
    /// Content for the agent layer position (from `agent_config[agent].setup`)
    pub agent_setup: Option<String>,
    /// Content for the project layer position (from `setup` field)
    pub project_setup: Option<String>,
}

/// Where a Docker layer's content came from
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum LayerSource {
    /// Layer content from TOML config (setup/stack_config/agent_config)
    Config,
    /// Layer content from embedded assets
    AssetManager,
    /// Layer was empty (no content from any source)
    #[default]
    Empty,
}

impl std::fmt::Display for LayerSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LayerSource::Config => write!(f, "config"),
            LayerSource::AssetManager => write!(f, "asset"),
            LayerSource::Empty => write!(f, "empty"),
        }
    }
}

/// Track where each layer came from for dry-run output
#[derive(Debug, Default)]
pub struct LayerSources {
    pub stack: LayerSource,
    pub agent: LayerSource,
    pub project: LayerSource,
}

/// Composes multiple Docker layers into a complete Dockerfile
pub struct DockerComposer;

impl DockerComposer {
    /// Creates a new DockerComposer
    pub fn new() -> Self {
        Self
    }

    /// Compose a complete Dockerfile and associated files from the given configuration.
    ///
    /// When `overrides` is provided, inline config content takes priority over
    /// embedded asset lookups for the corresponding layer positions.
    /// When `resolved_config` is provided, config-driven template variables
    /// (e.g., `{{{SUDO}}}`) are populated from the resolved configuration.
    pub fn compose(
        &self,
        config: &DockerImageConfig,
        overrides: Option<&InlineLayerOverrides>,
        resolved_config: Option<&ResolvedConfig>,
    ) -> Result<ComposedDockerfile> {
        // Get the base template
        let base_dockerfile = crate::assets::embedded::get_dockerfile("base/default")
            .context("Failed to get base dockerfile")?;
        let base_template = String::from_utf8(base_dockerfile)
            .context("Failed to decode base dockerfile as UTF-8")?;

        // Build config-driven template variables from resolved config
        let config_vars = Self::build_config_vars(resolved_config);

        // Create template engine and render the Dockerfile
        let template_engine = DockerTemplateEngine::new(overrides);
        let (dockerfile_content, layer_sources) = template_engine.render_dockerfile(
            &base_template,
            Some(&config.stack),
            Some(&config.agent),
            Some(&config.project),
            &config_vars,
        )?;

        // Extract build arguments from the composed Dockerfile
        let build_args = self.extract_build_args(&dockerfile_content)?;

        Ok(ComposedDockerfile {
            dockerfile_content,
            build_args,
            image_tag: config.image_tag(),
            layer_sources,
        })
    }

    /// Build config-driven template variables from resolved configuration.
    ///
    /// This translates resolved config flags into Dockerfile content that gets
    /// injected via named placeholders in the base template (e.g., `{{{SUDO}}}`).
    fn build_config_vars(resolved_config: Option<&ResolvedConfig>) -> HashMap<String, String> {
        let mut vars = HashMap::new();

        let sudo_content = match resolved_config {
            Some(config) if config.sudo => concat!(
                "USER root\n",
                "RUN echo 'agent ALL=(ALL) NOPASSWD:ALL' > /etc/sudoers.d/agent && chmod 0440 /etc/sudoers.d/agent\n",
                "USER agent",
            ).to_string(),
            _ => String::new(),
        };
        vars.insert("SUDO".to_string(), sudo_content);

        let tailscale_content = match resolved_config {
            Some(config) if config.tailscale => Self::tailscale_layer(),
            _ => String::new(),
        };
        vars.insert("TAILSCALE".to_string(), tailscale_content);

        vars
    }

    /// Dockerfile content that installs Tailscale and its startup script.
    ///
    /// The snippet is compiled into the binary, so the error arm is a
    /// can't-happen guard. If it ever fired, the image would build **without**
    /// the startup script and the task would then fail at container start
    /// (`tsk-tailscale-up: not found`) — i.e. the failure moves from build time
    /// to run time; it does not silently run the agent without the tailnet.
    fn tailscale_layer() -> String {
        match crate::assets::embedded::get_dockerfile("features/tailscale").map(String::from_utf8) {
            Ok(Ok(content)) => content,
            _ => {
                eprintln!("Warning: Failed to load the embedded Tailscale Docker layer");
                String::new()
            }
        }
    }

    /// Extract build arguments from Dockerfile content
    fn extract_build_args(&self, dockerfile_content: &str) -> Result<HashSet<String>> {
        let mut build_args = HashSet::new();

        for line in dockerfile_content.lines() {
            let trimmed = line.trim();
            if trimmed.starts_with("ARG ")
                && let Some(arg_def) = trimmed.strip_prefix("ARG ")
            {
                // Handle ARG NAME or ARG NAME=default_value
                let arg_name = arg_def
                    .split_once('=')
                    .map(|(name, _)| name)
                    .unwrap_or(arg_def)
                    .trim();

                if !arg_name.is_empty() {
                    build_args.insert(arg_name.to_string());
                }
            }
        }

        Ok(build_args)
    }

    /// Validate that a composed Dockerfile is valid
    pub fn validate_dockerfile(&self, content: &str) -> Result<()> {
        let mut has_from = false;
        let mut has_workdir = false;
        let mut has_user = false;

        for line in content.lines() {
            let trimmed = line.trim();

            if trimmed.starts_with("FROM ") {
                has_from = true;
            } else if trimmed.starts_with("WORKDIR ") {
                has_workdir = true;
            } else if trimmed.starts_with("USER ") {
                has_user = true;
            }
        }

        if !has_from {
            return Err(anyhow::anyhow!(
                "Dockerfile must contain at least one FROM instruction"
            ));
        }

        if !has_workdir {
            return Err(anyhow::anyhow!(
                "Dockerfile should contain a WORKDIR instruction"
            ));
        }

        if !has_user {
            return Err(anyhow::anyhow!(
                "Dockerfile should contain a USER instruction for security"
            ));
        }

        Ok(())
    }
}

/// Result of composing Docker layers
#[derive(Debug)]
pub struct ComposedDockerfile {
    /// The complete Dockerfile content
    pub dockerfile_content: String,
    /// Build arguments extracted from the Dockerfile
    pub build_args: HashSet<String>,
    /// The Docker image tag for this composition
    pub image_tag: String,
    /// Where each layer's content came from
    pub layer_sources: LayerSources,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_composer() -> DockerComposer {
        DockerComposer::new()
    }

    #[test]
    fn test_extract_build_args() {
        let composer = create_test_composer();

        let dockerfile = r#"
FROM ubuntu:24.04
ARG GIT_USER_NAME
ARG GIT_USER_EMAIL=default@example.com
ARG BUILD_VERSION
RUN echo "Building..."
"#;

        let args = composer.extract_build_args(dockerfile).unwrap();
        assert!(args.contains("GIT_USER_NAME"));
        assert!(args.contains("GIT_USER_EMAIL"));
        assert!(args.contains("BUILD_VERSION"));
        assert_eq!(args.len(), 3);
    }

    #[test]
    fn test_validate_dockerfile() {
        let composer = create_test_composer();

        // Valid Dockerfile
        let valid = r#"
FROM ubuntu:24.04
WORKDIR /workspace
USER agent
RUN echo "Hello"
"#;
        assert!(composer.validate_dockerfile(valid).is_ok());

        // Missing FROM
        let no_from = r#"
WORKDIR /workspace
USER agent
RUN echo "Hello"
"#;
        assert!(composer.validate_dockerfile(no_from).is_err());

        // Missing WORKDIR
        let no_workdir = r#"
FROM ubuntu:24.04
USER agent
RUN echo "Hello"
"#;
        assert!(composer.validate_dockerfile(no_workdir).is_err());

        // Missing USER
        let no_user = r#"
FROM ubuntu:24.04
WORKDIR /workspace
RUN echo "Hello"
"#;
        assert!(composer.validate_dockerfile(no_user).is_err());
    }

    #[test]
    fn test_compose_with_no_overrides() {
        let composer = create_test_composer();
        let config = crate::docker::layers::DockerImageConfig::new(
            "default".to_string(),
            "claude".to_string(),
            "default".to_string(),
        );

        let composed = composer.compose(&config, None, None).unwrap();
        assert!(composed.dockerfile_content.contains("FROM"));
        assert!(!composed.image_tag.is_empty());
        // Without overrides, embedded sources should be used
        assert_eq!(composed.layer_sources.stack, LayerSource::AssetManager);
        assert_eq!(composed.layer_sources.agent, LayerSource::AssetManager);
    }

    #[test]
    fn test_compose_with_inline_overrides() {
        let composer = create_test_composer();
        let config = crate::docker::layers::DockerImageConfig::new(
            "default".to_string(),
            "claude".to_string(),
            "default".to_string(),
        );

        let overrides = InlineLayerOverrides {
            stack_setup: Some("RUN echo custom-stack".to_string()),
            project_setup: Some("RUN echo custom-project".to_string()),
            ..Default::default()
        };

        let composed = composer.compose(&config, Some(&overrides), None).unwrap();
        assert!(
            composed
                .dockerfile_content
                .contains("RUN echo custom-stack")
        );
        assert!(
            composed
                .dockerfile_content
                .contains("RUN echo custom-project")
        );
        assert_eq!(composed.layer_sources.stack, LayerSource::Config);
        assert_eq!(composed.layer_sources.agent, LayerSource::AssetManager);
        assert_eq!(composed.layer_sources.project, LayerSource::Config);
    }

    #[test]
    fn test_compose_with_config_defined_new_stack() {
        let composer = create_test_composer();
        // Use a stack name that doesn't exist in embedded assets
        let config = crate::docker::layers::DockerImageConfig::new(
            "scala".to_string(),
            "claude".to_string(),
            "default".to_string(),
        );

        let overrides = InlineLayerOverrides {
            stack_setup: Some("RUN apt-get install -y scala".to_string()),
            ..Default::default()
        };

        let composed = composer.compose(&config, Some(&overrides), None).unwrap();
        assert!(
            composed
                .dockerfile_content
                .contains("RUN apt-get install -y scala")
        );
        assert_eq!(composed.layer_sources.stack, LayerSource::Config);
        // Agent still comes from embedded assets
        assert_eq!(composed.layer_sources.agent, LayerSource::AssetManager);
    }

    #[test]
    fn test_compose_with_sudo_injects_sudoers() {
        let composer = create_test_composer();
        let config = crate::docker::layers::DockerImageConfig::new(
            "default".to_string(),
            "claude".to_string(),
            "default".to_string(),
        );

        let resolved = ResolvedConfig {
            sudo: true,
            ..Default::default()
        };

        let composed = composer.compose(&config, None, Some(&resolved)).unwrap();
        assert!(
            composed.dockerfile_content.contains("NOPASSWD:ALL"),
            "Sudo injection should add NOPASSWD sudoers rule"
        );
    }

    #[test]
    fn test_compose_with_tailscale_injects_layer() {
        let composer = create_test_composer();
        let config = crate::docker::layers::DockerImageConfig::new(
            "default".to_string(),
            "claude".to_string(),
            "default".to_string(),
        );

        let resolved = ResolvedConfig {
            tailscale: true,
            ..Default::default()
        };

        let composed = composer.compose(&config, None, Some(&resolved)).unwrap();
        assert!(
            composed.dockerfile_content.contains("tailscaled"),
            "Tailscale layer should install tailscaled"
        );
        assert!(
            composed
                .dockerfile_content
                .contains("/usr/local/bin/tsk-tailscale-up"),
            "Tailscale layer should install the startup script"
        );

        // Disabled by default: no Tailscale content in the Dockerfile
        let composed = composer
            .compose(&config, None, Some(&ResolvedConfig::default()))
            .unwrap();
        assert!(!composed.dockerfile_content.contains("tailscale"));
    }

    #[test]
    fn test_compose_without_sudo_has_no_sudoers() {
        let composer = create_test_composer();
        let config = crate::docker::layers::DockerImageConfig::new(
            "default".to_string(),
            "claude".to_string(),
            "default".to_string(),
        );

        let resolved = ResolvedConfig::default();

        let composed = composer.compose(&config, None, Some(&resolved)).unwrap();
        assert!(
            !composed.dockerfile_content.contains("NOPASSWD"),
            "Without sudo, sudoers rule should not be present"
        );
    }
}
