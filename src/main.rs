#![deny(clippy::disallowed_methods)]
use clap::{Args, Parser, Subcommand};

mod agent;
mod assets;
mod commands;
mod context;
mod display;
mod docker;
mod file_system;
mod git;
mod git_operations;
mod git_sync;
mod repo_utils;
mod repository;
mod server;
mod stdin_utils;
mod task;
mod task_builder;
mod task_manager;
mod task_runner;
#[cfg(test)]
mod test_utils;
mod tui;
mod utils;

use commands::{
    AddCommand, CancelCommand, CleanCommand, Command, DeleteCommand, ListCommand, RetryCommand,
    ReviewCommand, RunCommand, ShellCommand,
    docker::DockerBuildCommand,
    server::{ServerStartCommand, ServerStopCommand},
    task_args::{self, TaskArgs},
    template::{TemplateEditCommand, TemplateListCommand, TemplateShowCommand},
};
use context::{AppContext, ContainerEngine};

#[derive(Parser)]
#[command(name = "tsk")]
#[command(author, version, about = "tsk-tsk: keeping your agents out of trouble with sandboxed development workflows", long_about = None)]
#[command(help_template = r#"{name} {version}
{author-with-newline}
{about-with-newline}
{usage-heading} {usage}

{all-args}{after-help}
"#)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Args, Clone)]
struct ContainerEngineArgs {
    /// Container engine to use (docker or podman)
    #[arg(long)]
    container_engine: Option<ContainerEngine>,
}

#[derive(Subcommand)]
#[command(about = "Task operations and configuration management", long_about = None)]
enum Commands {
    /// Immediately run a task in a sandbox container synchronously
    Run {
        #[command(flatten)]
        engine: ContainerEngineArgs,

        /// Unique identifier for the task (defaults to task type if not specified)
        #[arg(short, long)]
        name: Option<String>,

        /// Task type (defaults to 'generic' if not specified)
        #[arg(short = 't', long, default_value = "generic")]
        r#type: String,

        /// Task prompt describing what needs to be accomplished (can also be piped via stdin)
        #[arg(short = 'p', long = "prompt", conflicts_with = "prompt_file")]
        prompt: Option<String>,

        /// [deprecated: use --prompt/-p] Task prompt (deprecated alias)
        #[arg(
            short = 'd',
            long = "description",
            hide = true,
            conflicts_with = "prompt_file"
        )]
        description: Option<String>,

        /// Path to a file containing the task prompt (contents are injected into the template)
        #[arg(long = "prompt-file")]
        prompt_file: Option<String>,

        /// Open the prompt file in $EDITOR after creation
        #[arg(short, long)]
        edit: bool,

        /// Specific agent to use (claude, codex)
        #[arg(short, long)]
        agent: Option<String>,

        /// Stack for Docker image (e.g., rust, python, node)
        #[arg(long)]
        stack: Option<String>,

        /// Project name for Docker image
        #[arg(long)]
        project: Option<String>,

        /// Path to the git repository (defaults to current directory)
        #[arg(long)]
        repo: Option<String>,

        /// Disable per-container network isolation (allows direct internet access)
        #[arg(long)]
        no_network_isolation: bool,

        /// Enable Docker-in-Docker support (relaxes container security for nested builds)
        #[arg(long)]
        dind: bool,
    },
    /// Launch a sandbox container with an agent for interactive use
    Shell {
        #[command(flatten)]
        engine: ContainerEngineArgs,

        /// Unique identifier for the shell session
        #[arg(short, long, default_value = "shell")]
        name: String,

        /// Task type (defaults to 'shell' if not specified)
        #[arg(short = 't', long, default_value = "shell")]
        r#type: String,

        /// Task prompt describing what needs to be accomplished (can also be piped via stdin)
        #[arg(short = 'p', long = "prompt", conflicts_with = "prompt_file")]
        prompt: Option<String>,

        /// [deprecated: use --prompt/-p] Task prompt (deprecated alias)
        #[arg(
            short = 'd',
            long = "description",
            hide = true,
            conflicts_with = "prompt_file"
        )]
        description: Option<String>,

        /// Path to a file containing the task prompt (contents are injected into the template)
        #[arg(long = "prompt-file")]
        prompt_file: Option<String>,

        /// Open the prompt file in $EDITOR after creation
        #[arg(short, long)]
        edit: bool,

        /// Specific agent to use (defaults to claude)
        #[arg(short, long)]
        agent: Option<String>,

        /// Stack for Docker image (e.g., rust, python, node)
        #[arg(long, alias = "tech-stack")]
        stack: Option<String>,

        /// Project name for Docker image
        #[arg(long)]
        project: Option<String>,

        /// Path to the git repository (defaults to current directory)
        #[arg(long)]
        repo: Option<String>,

        /// Disable per-container network isolation (allows direct internet access)
        #[arg(long)]
        no_network_isolation: bool,

        /// Enable Docker-in-Docker support (relaxes container security for nested builds)
        #[arg(long)]
        dind: bool,
    },
    /// Queue a task for later execution by the TSK server
    Add {
        /// Unique identifier for the task (defaults to task type if not specified)
        #[arg(short, long)]
        name: Option<String>,

        /// Task type (defaults to 'generic' if not specified)
        #[arg(short = 't', long, default_value = "generic")]
        r#type: String,

        /// Task prompt describing what needs to be accomplished (can also be piped via stdin)
        #[arg(short = 'p', long = "prompt", conflicts_with = "prompt_file")]
        prompt: Option<String>,

        /// [deprecated: use --prompt/-p] Task prompt (deprecated alias)
        #[arg(
            short = 'd',
            long = "description",
            hide = true,
            conflicts_with = "prompt_file"
        )]
        description: Option<String>,

        /// Path to a file containing the task prompt (contents are injected into the template)
        #[arg(long = "prompt-file")]
        prompt_file: Option<String>,

        /// Open the prompt file in $EDITOR after creation
        #[arg(short, long)]
        edit: bool,

        /// Specific agent to use (claude, codex)
        #[arg(short, long)]
        agent: Option<String>,

        /// Stack for Docker image (e.g., rust, python, node)
        #[arg(long)]
        stack: Option<String>,

        /// Project name for Docker image
        #[arg(long)]
        project: Option<String>,

        /// Path to the git repository (defaults to current directory)
        #[arg(long)]
        repo: Option<String>,

        /// Parent task ID (task will wait for parent to complete before starting)
        #[arg(long = "parent")]
        parent_id: Option<String>,

        /// Disable per-container network isolation (allows direct internet access)
        #[arg(long)]
        no_network_isolation: bool,

        /// Enable Docker-in-Docker support (relaxes container security for nested builds)
        #[arg(long)]
        dind: bool,
    },
    /// Start or stop the TSK server daemon that runs queued tasks in containers
    Server(ServerArgs),
    /// List all queued tasks
    List,
    /// Cancel one or more running or queued tasks by ID
    Cancel {
        #[command(flatten)]
        engine: ContainerEngineArgs,

        /// Task IDs to cancel
        task_ids: Vec<String>,
    },
    /// Delete all completed tasks
    Clean,
    /// Delete one or more tasks by ID
    Delete {
        /// Task IDs to delete
        task_ids: Vec<String>,
    },
    /// Retry one or more tasks by creating new tasks with the same instructions
    Retry {
        #[command(flatten)]
        engine: ContainerEngineArgs,

        /// Task IDs to retry
        task_ids: Vec<String>,
        /// Unique identifier for the task (defaults to task type if not specified)
        #[arg(short, long)]
        name: Option<String>,
        /// Open the prompt file in $EDITOR after creation
        #[arg(short, long)]
        edit: bool,
        /// Specific agent to use (claude, codex)
        #[arg(short, long)]
        agent: Option<String>,
        /// Stack for Docker image (e.g., rust, python, node)
        #[arg(long)]
        stack: Option<String>,
        /// Project name for Docker image
        #[arg(long)]
        project: Option<String>,
        /// Parent task ID (task will wait for parent to complete before starting)
        #[arg(long = "parent")]
        parent_id: Option<String>,

        /// Enable Docker-in-Docker support (relaxes container security for nested builds)
        #[arg(long)]
        dind: bool,

        /// Skip retrying child tasks (don't prompt)
        #[arg(long)]
        no_children: bool,

        /// Use current working directory instead of parent task's repository
        #[arg(long)]
        from_cwd: bool,
    },
    /// Docker operations - build and manage TSK Docker images
    Docker(DockerArgs),
    /// Open a completed task's changes for review
    Review {
        /// Task ID to review
        task_id: Option<String>,

        /// Git ref to review against (mutually exclusive with task_id)
        #[arg(long)]
        base: Option<String>,

        /// Override the review task name
        #[arg(short, long)]
        name: Option<String>,

        /// Specific agent to use
        #[arg(short, long)]
        agent: Option<String>,

        /// Stack for Docker image
        #[arg(long)]
        stack: Option<String>,

        /// Path to the git repository
        #[arg(long)]
        repo: Option<String>,

        /// Open the full prompt in editor after review file is processed
        #[arg(short, long)]
        edit: bool,

        /// Disable per-container network isolation
        #[arg(long)]
        no_network_isolation: bool,

        /// Enable Docker-in-Docker support
        #[arg(long)]
        dind: bool,
    },
    /// Template operations - manage task templates
    Template(TemplateArgs),
}

impl Commands {
    fn container_engine(&self) -> Option<ContainerEngine> {
        match self {
            Commands::Run { engine, .. } => engine.container_engine.clone(),
            Commands::Shell { engine, .. } => engine.container_engine.clone(),
            Commands::Add { .. } => None,
            Commands::Retry { engine, .. } => engine.container_engine.clone(),
            Commands::Server(args) => match &args.command {
                ServerCommands::Start { engine, .. } => engine.container_engine.clone(),
                ServerCommands::Stop => None,
            },
            Commands::Docker(args) => match &args.command {
                DockerCommands::Build { engine, .. } => engine.container_engine.clone(),
            },
            Commands::Cancel { engine, .. } => engine.container_engine.clone(),
            Commands::List
            | Commands::Clean
            | Commands::Delete { .. }
            | Commands::Review { .. }
            | Commands::Template(_) => None,
        }
    }
}

#[derive(Args)]
#[command(about = "Manage the tsk server daemon")]
struct ServerArgs {
    #[command(subcommand)]
    command: ServerCommands,
}

#[derive(Subcommand)]
enum ServerCommands {
    /// Start the tsk server daemon
    Start {
        #[command(flatten)]
        engine: ContainerEngineArgs,

        /// Number of parallel workers for task execution
        #[arg(short, long, default_value = "1", value_parser = clap::value_parser!(u32).range(1..=32))]
        workers: u32,

        /// Quit when done - exit when queue is empty and all tasks complete
        #[arg(short, long)]
        quit: bool,

        /// Play sound with task completion notifications
        #[arg(short, long)]
        sound: bool,
    },
    /// Stop the running tsk server
    Stop,
}

#[derive(Args)]
#[command(about = "Build and manage tsk Docker images")]
struct DockerArgs {
    #[command(subcommand)]
    command: DockerCommands,
}

#[derive(Subcommand)]
enum DockerCommands {
    /// Build TSK Docker images
    Build {
        #[command(flatten)]
        engine: ContainerEngineArgs,

        /// Build without using Docker's cache
        #[arg(long)]
        no_cache: bool,

        /// Stack (e.g., rust, python, node)
        #[arg(long)]
        stack: Option<String>,

        /// Agent (e.g., claude, codex)
        #[arg(long)]
        agent: Option<String>,

        /// Project name
        #[arg(long)]
        project: Option<String>,

        /// Print the resolved Dockerfile without building
        #[arg(long)]
        dry_run: bool,

        /// Only build the proxy image (skip project/stack/agent images)
        #[arg(long, conflicts_with_all = ["stack", "agent", "project", "dry_run"])]
        proxy_only: bool,
    },
}

#[derive(Args)]
#[command(about = "Manage task templates")]
struct TemplateArgs {
    #[command(subcommand)]
    command: TemplateCommands,
}

#[derive(Subcommand)]
enum TemplateCommands {
    /// List available task templates and their sources
    List,
    /// Display the contents of a template
    Show {
        /// Template name
        name: String,
    },
    /// Open a template in your editor
    Edit {
        /// Template name
        name: String,
    },
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();

    // Create the AppContext using the builder pattern
    let app_context = AppContext::builder()
        .with_container_engine(cli.command.container_engine())
        .build();

    // Warn about deprecated dockerfile directories and old config format
    let project_root = repo_utils::find_repository_root(std::path::Path::new(".")).ok();
    assets::warn_deprecated_dockerfiles(project_root.as_deref(), &app_context.tsk_env());

    let command: Box<dyn Command> = match cli.command {
        Commands::Add {
            name,
            r#type,
            description,
            prompt,
            prompt_file,
            edit,
            agent,
            stack,
            project,
            repo,
            parent_id,
            no_network_isolation,
            dind,
        } => {
            let prompt = task_args::resolve_deprecation(prompt, description).unwrap_or_else(|e| {
                eprintln!("Error: {e}");
                std::process::exit(1);
            });
            Box::new(AddCommand {
                task_args: TaskArgs {
                    name,
                    r#type,
                    prompt,
                    prompt_file,
                    edit,
                    agent,
                    stack,
                    project,
                    repo,
                    no_network_isolation,
                    dind,
                    target_branch: None,
                },
                parent_id,
            })
        }
        Commands::Run {
            engine: _,
            name,
            r#type,
            description,
            prompt,
            prompt_file,
            edit,
            agent,
            stack,
            project,
            repo,
            no_network_isolation,
            dind,
        } => {
            let prompt = task_args::resolve_deprecation(prompt, description).unwrap_or_else(|e| {
                eprintln!("Error: {e}");
                std::process::exit(1);
            });
            Box::new(RunCommand {
                task_args: TaskArgs {
                    name,
                    r#type,
                    prompt,
                    prompt_file,
                    edit,
                    agent,
                    stack,
                    project,
                    repo,
                    no_network_isolation,
                    dind,
                    target_branch: None,
                },
                docker_client_override: None,
            })
        }
        Commands::Shell {
            engine: _,
            name,
            r#type,
            description,
            prompt,
            prompt_file,
            edit,
            agent,
            stack,
            project,
            repo,
            no_network_isolation,
            dind,
        } => {
            let prompt = task_args::resolve_deprecation(prompt, description).unwrap_or_else(|e| {
                eprintln!("Error: {e}");
                std::process::exit(1);
            });
            Box::new(ShellCommand {
                task_args: TaskArgs {
                    name: Some(name),
                    r#type,
                    prompt,
                    prompt_file,
                    edit,
                    agent,
                    stack,
                    project,
                    repo,
                    no_network_isolation,
                    dind,
                    target_branch: None,
                },
            })
        }
        Commands::Cancel {
            engine: _,
            task_ids,
        } => Box::new(CancelCommand {
            task_ids,
            docker_client_override: None,
        }),
        Commands::List => Box::new(ListCommand),
        Commands::Clean => Box::new(CleanCommand),
        Commands::Delete { task_ids } => Box::new(DeleteCommand { task_ids }),
        Commands::Retry {
            engine: _,
            task_ids,
            name,
            edit,
            agent,
            stack,
            project,
            parent_id,
            dind,
            no_children,
            from_cwd,
        } => Box::new(RetryCommand {
            task_ids,
            edit,
            name,
            agent,
            stack,
            project,
            parent_id,
            dind: if dind { Some(true) } else { None },
            no_children,
            from_cwd,
        }),
        Commands::Server(server_args) => match server_args.command {
            ServerCommands::Start {
                engine: _,
                workers,
                quit,
                sound,
            } => Box::new(ServerStartCommand {
                workers,
                quit,
                sound,
            }),
            ServerCommands::Stop => Box::new(ServerStopCommand),
        },
        Commands::Docker(docker_args) => match docker_args.command {
            DockerCommands::Build {
                engine: _,
                no_cache,
                stack,
                agent,
                project,
                dry_run,
                proxy_only,
            } => Box::new(DockerBuildCommand {
                no_cache,
                stack,
                agent,
                project,
                dry_run,
                proxy_only,
            }),
        },
        Commands::Review {
            task_id,
            base,
            name,
            agent,
            stack,
            repo,
            edit,
            no_network_isolation,
            dind,
        } => Box::new(ReviewCommand {
            task_id,
            base,
            name,
            agent,
            stack,
            repo,
            edit,
            no_network_isolation,
            dind,
        }),
        Commands::Template(template_args) => match template_args.command {
            TemplateCommands::List => Box::new(TemplateListCommand),
            TemplateCommands::Show { name } => Box::new(TemplateShowCommand { name }),
            TemplateCommands::Edit { name } => Box::new(TemplateEditCommand { name }),
        },
    };

    if let Err(e) = command.execute(&app_context).await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
