use super::app::{Panel, TuiApp};
use super::ui::task_display_height;
use crate::task::TaskStatus;
use crossterm::event::{Event, KeyCode, KeyEventKind, MouseButton, MouseEventKind};
use std::path::{Path, PathBuf};

/// Action returned by handle_event for the async event loop to execute.
pub enum TuiAction {
    CancelTask {
        task_id: String,
        was_running: bool,
        is_interactive: bool,
    },
    DeleteTask {
        task_id: String,
        task_dir: Option<PathBuf>,
    },
}

/// Handle a crossterm event, updating TUI application state accordingly.
///
/// Processes keyboard navigation (vim-style and arrow keys) for panel
/// switching, task selection, and log scrolling. Also handles mouse
/// scroll events, routing to the appropriate panel based on cursor position.
///
/// Returns a [`TuiAction`] when the event triggers an async operation
/// (cancel or delete) that the caller must execute.
pub fn handle_event(app: &mut TuiApp, event: &Event, data_dir: &Path) -> Option<TuiAction> {
    match event {
        Event::Key(key) if key.kind == KeyEventKind::Press => match key.code {
            KeyCode::Char('q') => {
                app.should_quit = true;
            }
            KeyCode::Char('h') | KeyCode::Left => {
                app.focus = Panel::Tasks;
            }
            KeyCode::Char('l') | KeyCode::Right => {
                app.focus = Panel::Logs;
            }
            KeyCode::Char('j') | KeyCode::Down => match app.focus {
                Panel::Tasks => {
                    app.select_next_task();
                    app.load_logs_for_selected_task(data_dir);
                }
                Panel::Logs => {
                    app.scroll_logs_down(1);
                }
            },
            KeyCode::Char('k') | KeyCode::Up => match app.focus {
                Panel::Tasks => {
                    app.select_previous_task();
                    app.load_logs_for_selected_task(data_dir);
                }
                Panel::Logs => {
                    app.scroll_logs_up(1);
                }
            },
            KeyCode::PageDown if app.focus == Panel::Logs => {
                app.scroll_logs_down(20);
            }
            KeyCode::PageUp if app.focus == Panel::Logs => {
                app.scroll_logs_up(20);
            }
            KeyCode::Char('c') => {
                if app.focus == Panel::Tasks
                    && let Some(idx) = app.task_list_state.selected()
                    && let Some(task) = app.tasks.get(idx)
                {
                    match task.status {
                        TaskStatus::Queued | TaskStatus::Running => {
                            return Some(TuiAction::CancelTask {
                                task_id: task.id.clone(),
                                was_running: task.status == TaskStatus::Running,
                                is_interactive: task.is_interactive,
                            });
                        }
                        _ => {}
                    }
                }
            }
            KeyCode::Char('d') => {
                if app.focus == Panel::Tasks
                    && let Some(idx) = app.task_list_state.selected()
                    && let Some(task) = app.tasks.get(idx)
                {
                    match task.status {
                        TaskStatus::Complete | TaskStatus::Failed | TaskStatus::Cancelled => {
                            let task_dir = task
                                .copied_repo_path
                                .as_ref()
                                .and_then(|p| p.parent())
                                .map(|p| p.to_path_buf());
                            return Some(TuiAction::DeleteTask {
                                task_id: task.id.clone(),
                                task_dir,
                            });
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        },
        Event::Mouse(mouse) => {
            let in_task_panel = mouse.column < app.task_panel_width;

            match mouse.kind {
                MouseEventKind::ScrollDown => {
                    if in_task_panel {
                        app.select_next_task();
                        app.load_logs_for_selected_task(data_dir);
                    } else {
                        app.scroll_logs_down(3);
                    }
                }
                MouseEventKind::ScrollUp => {
                    if in_task_panel {
                        app.select_previous_task();
                        app.load_logs_for_selected_task(data_dir);
                    } else {
                        app.scroll_logs_up(3);
                    }
                }
                MouseEventKind::Down(MouseButton::Left) => {
                    if in_task_panel
                        && mouse.column == app.task_panel_width.saturating_sub(1)
                        && mouse.row >= app.task_list_top
                        && mouse.row < app.task_list_top + app.task_list_height
                    {
                        // Click on the scrollbar track column
                        app.task_scrollbar_drag = true;
                        app.focus = Panel::Tasks;
                        let offset = scrollbar_row_to_offset(mouse.row, app);
                        app.scroll_task_list_to_offset(offset);
                        app.load_logs_for_selected_task(data_dir);
                    } else if in_task_panel {
                        app.focus = Panel::Tasks;
                        if let Some(inner_row) = mouse.row.checked_sub(app.task_list_top) {
                            let click_row = inner_row as usize;
                            let offset = app.task_list_state.offset();
                            let mut accumulated = 0;
                            let mut task_index = None;
                            for (i, task) in app.tasks.iter().enumerate().skip(offset) {
                                let height = task_display_height(task, &app.tasks) as usize;
                                if click_row < accumulated + height {
                                    task_index = Some(i);
                                    break;
                                }
                                accumulated += height;
                            }
                            if let Some(task_index) = task_index {
                                app.select_task(task_index);
                                app.load_logs_for_selected_task(data_dir);
                            }
                        }
                    } else {
                        app.focus = Panel::Logs;
                    }
                }
                MouseEventKind::Drag(MouseButton::Left) if app.task_scrollbar_drag => {
                    let prev_selected = app.task_list_state.selected();
                    let offset = scrollbar_row_to_offset(mouse.row, app);
                    app.scroll_task_list_to_offset(offset);
                    if app.task_list_state.selected() != prev_selected {
                        app.load_logs_for_selected_task(data_dir);
                    }
                }
                MouseEventKind::Up(MouseButton::Left) => {
                    app.task_scrollbar_drag = false;
                }
                _ => {}
            }
        }
        _ => {}
    }
    None
}

/// Map a mouse row on the scrollbar track to a task list scroll offset.
///
/// Inverts ratatui's scrollbar thumb position formula so the thumb tracks the
/// mouse exactly during click and drag. ratatui renders the thumb at:
///   `thumb_start = round(position * track / (max_offset + viewport_items))`
/// so the inverse is:
///   `position = round(click * (max_offset + viewport_items) / track)`
fn scrollbar_row_to_offset(row: u16, app: &TuiApp) -> usize {
    let track_height = app.task_list_height as usize;
    if track_height == 0 || app.tasks.is_empty() {
        return 0;
    }
    let viewport_items = app.task_viewport_items();
    let max_offset = app.tasks.len().saturating_sub(viewport_items);
    if max_offset == 0 {
        return 0;
    }
    let click_offset = row.saturating_sub(app.task_list_top) as usize;
    let click_offset = click_offset.min(track_height.saturating_sub(1));
    // Inverse of ratatui's thumb position formula (see ScrollbarState config in ui.rs)
    let max_viewport_position = max_offset + viewport_items;
    let offset = (click_offset * max_viewport_position + track_height / 2) / track_height;
    offset.min(max_offset)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::Task;
    use crossterm::event::{
        KeyCode, KeyEvent, KeyEventKind, KeyEventState, KeyModifiers, MouseButton, MouseEvent,
        MouseEventKind,
    };
    use std::fs;

    fn make_key_event(code: KeyCode) -> Event {
        Event::Key(KeyEvent {
            code,
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Press,
            state: KeyEventState::NONE,
        })
    }

    fn make_mouse_scroll(kind: MouseEventKind, column: u16) -> Event {
        Event::Mouse(MouseEvent {
            kind,
            column,
            row: 5,
            modifiers: KeyModifiers::NONE,
        })
    }

    fn make_mouse_click(column: u16, row: u16) -> Event {
        Event::Mouse(MouseEvent {
            kind: MouseEventKind::Down(MouseButton::Left),
            column,
            row,
            modifiers: KeyModifiers::NONE,
        })
    }

    fn make_mouse_drag(column: u16, row: u16) -> Event {
        Event::Mouse(MouseEvent {
            kind: MouseEventKind::Drag(MouseButton::Left),
            column,
            row,
            modifiers: KeyModifiers::NONE,
        })
    }

    fn make_mouse_up(column: u16, row: u16) -> Event {
        Event::Mouse(MouseEvent {
            kind: MouseEventKind::Up(MouseButton::Left),
            column,
            row,
            modifiers: KeyModifiers::NONE,
        })
    }

    /// Create a TuiApp with many tasks for scrollbar testing.
    ///
    /// Sets up 20 tasks with task_panel_width=30, task_list_top=2,
    /// and task_list_height=6 (viewport_items=3).
    fn app_with_many_tasks() -> TuiApp {
        let mut app = TuiApp::new(2);
        app.tasks = (0..20)
            .map(|i| Task {
                id: format!("t{i}"),
                name: format!("task-{i}"),
                branch_name: format!("tsk/feat/task-{i}/t{i}"),
                ..Task::test_default()
            })
            .collect();
        app.task_panel_width = 30;
        app.task_list_top = 2;
        app.task_list_height = 6; // viewport_items = 6/2 = 3
        app
    }

    fn app_with_tasks() -> TuiApp {
        let mut app = TuiApp::new(2);
        app.tasks = vec![
            Task {
                id: "t1".to_string(),
                name: "task-1".to_string(),
                branch_name: "tsk/feat/task-1/t1".to_string(),
                ..Task::test_default()
            },
            Task {
                id: "t2".to_string(),
                name: "task-2".to_string(),
                branch_name: "tsk/feat/task-2/t2".to_string(),
                ..Task::test_default()
            },
            Task {
                id: "t3".to_string(),
                name: "task-3".to_string(),
                branch_name: "tsk/feat/task-3/t3".to_string(),
                ..Task::test_default()
            },
        ];
        app
    }

    #[test]
    fn test_quit() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = TuiApp::new(1);
        assert!(!app.should_quit);

        handle_event(&mut app, &make_key_event(KeyCode::Char('q')), tmp.path());
        assert!(app.should_quit);
    }

    #[test]
    fn test_panel_switching() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = TuiApp::new(1);
        assert_eq!(app.focus, Panel::Tasks);

        handle_event(&mut app, &make_key_event(KeyCode::Char('l')), tmp.path());
        assert_eq!(app.focus, Panel::Logs);

        handle_event(&mut app, &make_key_event(KeyCode::Char('h')), tmp.path());
        assert_eq!(app.focus, Panel::Tasks);

        handle_event(&mut app, &make_key_event(KeyCode::Right), tmp.path());
        assert_eq!(app.focus, Panel::Logs);

        handle_event(&mut app, &make_key_event(KeyCode::Left), tmp.path());
        assert_eq!(app.focus, Panel::Tasks);
    }

    #[test]
    fn test_task_navigation_with_j_k() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_tasks();

        assert_eq!(app.task_list_state.selected(), Some(0));

        handle_event(&mut app, &make_key_event(KeyCode::Char('j')), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(1));

        handle_event(&mut app, &make_key_event(KeyCode::Char('k')), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(0));
    }

    #[test]
    fn test_task_navigation_with_arrows() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_tasks();

        handle_event(&mut app, &make_key_event(KeyCode::Down), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(1));

        handle_event(&mut app, &make_key_event(KeyCode::Up), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(0));
    }

    #[test]
    fn test_log_scrolling() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = TuiApp::new(1);
        app.focus = Panel::Logs;
        app.log_content = (0..100)
            .map(|i| crate::agent::log_line::LogLine::message(vec![], None, format!("line {i}")))
            .collect();
        app.log_viewport_height = 10;
        app.log_wrapped_line_count = 100;

        handle_event(&mut app, &make_key_event(KeyCode::Char('j')), tmp.path());
        assert_eq!(app.log_scroll, 1);

        handle_event(&mut app, &make_key_event(KeyCode::Char('k')), tmp.path());
        assert_eq!(app.log_scroll, 0);

        handle_event(&mut app, &make_key_event(KeyCode::PageDown), tmp.path());
        assert_eq!(app.log_scroll, 20);

        handle_event(&mut app, &make_key_event(KeyCode::PageUp), tmp.path());
        assert_eq!(app.log_scroll, 0);
    }

    #[test]
    fn test_task_selection_loads_logs() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path();

        // Create log files for tasks in JSON-lines format
        let log_dir = data_dir.join("tasks").join("t2").join("output");
        fs::create_dir_all(&log_dir).unwrap();
        let log_line = crate::agent::log_line::LogLine::message(vec![], None, "log from t2".into());
        let json = serde_json::to_string(&log_line).unwrap();
        fs::write(log_dir.join("agent.log"), format!("{json}\n")).unwrap();

        let mut app = app_with_tasks();

        handle_event(&mut app, &make_key_event(KeyCode::Char('j')), data_dir);
        assert_eq!(app.task_list_state.selected(), Some(1));
        assert_eq!(app.log_content.len(), 1);
        assert_eq!(
            app.log_content,
            vec![crate::agent::log_line::LogLine::message(
                vec![],
                None,
                "log from t2".into()
            )]
        );
    }

    #[test]
    fn test_mouse_scroll_in_task_panel() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_tasks();
        app.task_panel_width = 30;

        // Column 0 is within the task panel
        let scroll_down = make_mouse_scroll(MouseEventKind::ScrollDown, 0);
        handle_event(&mut app, &scroll_down, tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(1));

        let scroll_up = make_mouse_scroll(MouseEventKind::ScrollUp, 0);
        handle_event(&mut app, &scroll_up, tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(0));
    }

    #[test]
    fn test_mouse_scroll_in_log_panel() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = TuiApp::new(1);
        app.task_panel_width = 30;
        app.log_content = (0..100)
            .map(|i| crate::agent::log_line::LogLine::message(vec![], None, format!("line {i}")))
            .collect();
        app.log_viewport_height = 10;
        app.log_wrapped_line_count = 100;

        // Column 79 is well past the task panel boundary
        let scroll_down = make_mouse_scroll(MouseEventKind::ScrollDown, 79);
        handle_event(&mut app, &scroll_down, tmp.path());
        assert_eq!(app.log_scroll, 3);

        let scroll_up = make_mouse_scroll(MouseEventKind::ScrollUp, 79);
        handle_event(&mut app, &scroll_up, tmp.path());
        assert_eq!(app.log_scroll, 0);
    }

    #[test]
    fn test_page_keys_only_work_in_logs_panel() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = TuiApp::new(1);
        app.focus = Panel::Tasks;

        // PageDown/PageUp should be ignored when tasks panel is focused
        handle_event(&mut app, &make_key_event(KeyCode::PageDown), tmp.path());
        assert_eq!(app.log_scroll, 0);

        handle_event(&mut app, &make_key_event(KeyCode::PageUp), tmp.path());
        assert_eq!(app.log_scroll, 0);
    }

    #[test]
    fn test_mouse_click_selects_task() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_tasks();
        app.task_panel_width = 30;
        app.task_list_top = 2; // header(1) + top border(1)

        // Click on the second task (rows 4-5, each task is 2 rows tall)
        handle_event(&mut app, &make_mouse_click(5, 4), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(1));
        assert_eq!(app.focus, Panel::Tasks);

        // Click on the third task (rows 6-7)
        handle_event(&mut app, &make_mouse_click(5, 6), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(2));
    }

    #[test]
    fn test_mouse_click_loads_logs() {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path();

        let log_dir = data_dir.join("tasks").join("t2").join("output");
        fs::create_dir_all(&log_dir).unwrap();
        let log_line = crate::agent::log_line::LogLine::message(vec![], None, "log from t2".into());
        let json = serde_json::to_string(&log_line).unwrap();
        fs::write(log_dir.join("agent.log"), format!("{json}\n")).unwrap();

        let mut app = app_with_tasks();
        app.task_panel_width = 30;
        app.task_list_top = 2;

        // Click on the second task
        handle_event(&mut app, &make_mouse_click(5, 4), data_dir);
        assert_eq!(app.task_list_state.selected(), Some(1));
        assert_eq!(app.log_content.len(), 1);
        assert_eq!(
            app.log_content,
            vec![crate::agent::log_line::LogLine::message(
                vec![],
                None,
                "log from t2".into()
            )]
        );
    }

    #[test]
    fn test_mouse_click_out_of_bounds_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_tasks();
        app.task_panel_width = 30;
        app.task_list_top = 2;

        // Click well below the last task (row 20, only 3 tasks at rows 2-7)
        handle_event(&mut app, &make_mouse_click(5, 20), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(0)); // unchanged

        // Click on the top border (row 1, task_list_top is 2)
        handle_event(&mut app, &make_mouse_click(5, 1), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(0)); // unchanged
    }

    #[test]
    fn test_mouse_click_empty_task_list() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = TuiApp::new(1);
        app.task_panel_width = 30;
        app.task_list_top = 2;

        // Click in the task area with no tasks — selection should not change
        handle_event(&mut app, &make_mouse_click(5, 3), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(0));
    }

    #[test]
    fn test_mouse_click_focuses_panel() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_tasks();
        app.task_panel_width = 30;
        app.task_list_top = 2;

        // Click on log panel (column 50 > task_panel_width 30)
        handle_event(&mut app, &make_mouse_click(50, 5), tmp.path());
        assert_eq!(app.focus, Panel::Logs);

        // Click on task panel focuses it back
        handle_event(&mut app, &make_mouse_click(5, 2), tmp.path());
        assert_eq!(app.focus, Panel::Tasks);
    }

    #[test]
    fn test_mouse_click_selects_task_with_mixed_heights() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = TuiApp::new(2);
        app.task_panel_width = 40;
        app.task_list_top = 2;

        // parent (2 lines), child (3 lines), standalone (2 lines)
        app.tasks = vec![
            Task {
                id: "parent".to_string(),
                name: "parent-task".to_string(),
                status: crate::task::TaskStatus::Running,
                branch_name: "tsk/feat/parent-task/parent".to_string(),
                ..Task::test_default()
            },
            Task {
                id: "child".to_string(),
                name: "child-task".to_string(),
                status: crate::task::TaskStatus::Queued,
                parent_ids: vec!["parent".to_string()],
                branch_name: "tsk/feat/child-task/child".to_string(),
                ..Task::test_default()
            },
            Task {
                id: "standalone".to_string(),
                name: "standalone-task".to_string(),
                status: crate::task::TaskStatus::Running,
                branch_name: "tsk/feat/standalone-task/standalone".to_string(),
                ..Task::test_default()
            },
        ];

        // Row layout (task_list_top=2):
        // Row 2-3: parent (2 lines, inner rows 0-1)
        // Row 4-6: child  (3 lines, inner rows 2-4)
        // Row 7-8: standalone (2 lines, inner rows 5-6)

        // Click on parent (inner row 0)
        handle_event(&mut app, &make_mouse_click(5, 2), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(0));

        // Click on parent second line (inner row 1)
        handle_event(&mut app, &make_mouse_click(5, 3), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(0));

        // Click on child first line (inner row 2)
        handle_event(&mut app, &make_mouse_click(5, 4), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(1));

        // Click on child second line (inner row 3)
        handle_event(&mut app, &make_mouse_click(5, 5), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(1));

        // Click on child third line (inner row 4)
        handle_event(&mut app, &make_mouse_click(5, 6), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(1));

        // Click on standalone first line (inner row 5)
        handle_event(&mut app, &make_mouse_click(5, 7), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(2));

        // Click on standalone second line (inner row 6)
        handle_event(&mut app, &make_mouse_click(5, 8), tmp.path());
        assert_eq!(app.task_list_state.selected(), Some(2));
    }

    #[test]
    fn test_release_key_events_ignored() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = TuiApp::new(1);

        let release_event = Event::Key(KeyEvent {
            code: KeyCode::Char('q'),
            modifiers: KeyModifiers::NONE,
            kind: KeyEventKind::Release,
            state: KeyEventState::NONE,
        });
        handle_event(&mut app, &release_event, tmp.path());
        assert!(!app.should_quit);
    }

    #[test]
    fn test_scrollbar_click_scrolls_to_position() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_many_tasks();
        // scrollbar column = task_panel_width - 1 = 29
        // task_list_top = 2, task_list_height = 6, so valid rows: 2..8
        // viewport_items = 3, max_offset = 20 - 3 = 17

        // Click at the top of the scrollbar track (row 2) -> offset 0
        handle_event(&mut app, &make_mouse_click(29, 2), tmp.path());
        assert_eq!(app.task_list_state.offset(), 0);
        assert_eq!(app.focus, Panel::Tasks);

        // Click at the bottom of the track (row 7 = task_list_top + task_list_height - 1)
        // click_offset = 7 - 2 = 5, round(5 * 20 / 6) = 17
        handle_event(&mut app, &make_mouse_click(29, 7), tmp.path());
        assert_eq!(app.task_list_state.offset(), 17);
        // Selection should be clamped to visible range [17..19]
        let selected = app.task_list_state.selected().unwrap();
        assert!((17..20).contains(&selected));

        // Click at midpoint (row 4) -> click_offset = 2, round(2 * 20 / 6) = 7
        handle_event(&mut app, &make_mouse_click(29, 4), tmp.path());
        assert_eq!(app.task_list_state.offset(), 7);
    }

    #[test]
    fn test_scrollbar_drag_updates_offset() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_many_tasks();

        // Start drag by clicking on scrollbar column
        handle_event(&mut app, &make_mouse_click(29, 2), tmp.path());
        assert!(app.task_scrollbar_drag);
        assert_eq!(app.task_list_state.offset(), 0);

        // Drag to middle of track (row 4) -> offset 7
        handle_event(&mut app, &make_mouse_drag(29, 4), tmp.path());
        assert_eq!(app.task_list_state.offset(), 7);

        // Drag to bottom (row 7) -> offset 17
        handle_event(&mut app, &make_mouse_drag(29, 7), tmp.path());
        assert_eq!(app.task_list_state.offset(), 17);

        // Drag state is still active
        assert!(app.task_scrollbar_drag);
    }

    #[test]
    fn test_scrollbar_click_does_not_affect_non_scrollbar_clicks() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_many_tasks();

        // Click on column 5 (not the scrollbar column 29) in the task area
        handle_event(&mut app, &make_mouse_click(5, 4), tmp.path());
        // Should do normal task selection (row 4, inner_row=2, task_index=1)
        assert_eq!(app.task_list_state.selected(), Some(1));
        assert!(!app.task_scrollbar_drag);
    }

    #[test]
    fn test_scrollbar_up_clears_drag() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_many_tasks();

        // Start drag
        handle_event(&mut app, &make_mouse_click(29, 3), tmp.path());
        assert!(app.task_scrollbar_drag);

        // Release mouse
        handle_event(&mut app, &make_mouse_up(29, 5), tmp.path());
        assert!(!app.task_scrollbar_drag);
    }

    #[test]
    fn test_scrollbar_to_top_with_mixed_height_tasks() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = TuiApp::new(2);

        // Create tasks with mixed heights: parents (2 rows) and children (3 rows).
        // The child tasks have visible parents so they display at 3 rows each.
        app.tasks = vec![
            Task {
                id: "p1".to_string(),
                name: "parent-1".to_string(),
                branch_name: "tsk/feat/parent-1/p1".to_string(),
                ..Task::test_default()
            },
            Task {
                id: "c1".to_string(),
                name: "child-1".to_string(),
                parent_ids: vec!["p1".to_string()],
                branch_name: "tsk/feat/child-1/c1".to_string(),
                ..Task::test_default()
            },
            Task {
                id: "p2".to_string(),
                name: "parent-2".to_string(),
                branch_name: "tsk/feat/parent-2/p2".to_string(),
                ..Task::test_default()
            },
            Task {
                id: "c2".to_string(),
                name: "child-2".to_string(),
                parent_ids: vec!["p2".to_string()],
                branch_name: "tsk/feat/child-2/c2".to_string(),
                ..Task::test_default()
            },
            Task {
                id: "p3".to_string(),
                name: "parent-3".to_string(),
                branch_name: "tsk/feat/parent-3/p3".to_string(),
                ..Task::test_default()
            },
            Task {
                id: "c3".to_string(),
                name: "child-3".to_string(),
                parent_ids: vec!["p3".to_string()],
                branch_name: "tsk/feat/child-3/c3".to_string(),
                ..Task::test_default()
            },
        ];

        // task_list_height=10 with mixed items: p1(2)+c1(3)+p2(2)+c2(3)=10
        // So 4 items fit, not task_list_height/2=5.
        app.task_panel_width = 30;
        app.task_list_top = 2;
        app.task_list_height = 10;

        // Start scrolled down with bottom visible task selected
        app.task_list_state.select(Some(3)); // c2 — last item that fits from offset 0
        *app.task_list_state.offset_mut() = 2;

        // Drag scrollbar to the very top (row = task_list_top)
        handle_event(&mut app, &make_mouse_click(29, 2), tmp.path());
        assert_eq!(
            app.task_list_state.offset(),
            0,
            "offset should be 0 after scrollbar click at top"
        );
        // Selection should be clamped to the last item that actually fits (index 3)
        assert!(
            app.task_list_state.selected().unwrap() <= 3,
            "selected task should fit in viewport from offset 0"
        );
    }

    #[test]
    fn test_cancel_running_task() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_tasks();
        app.tasks[0].status = TaskStatus::Running;
        app.focus = Panel::Tasks;

        let action = handle_event(&mut app, &make_key_event(KeyCode::Char('c')), tmp.path());
        match action {
            Some(TuiAction::CancelTask {
                task_id,
                was_running,
                is_interactive,
            }) => {
                assert_eq!(task_id, "t1");
                assert!(was_running);
                assert!(!is_interactive);
            }
            _ => panic!("Expected CancelTask action"),
        }
    }

    #[test]
    fn test_cancel_queued_task() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_tasks();
        app.tasks[0].status = TaskStatus::Queued;
        app.focus = Panel::Tasks;

        let action = handle_event(&mut app, &make_key_event(KeyCode::Char('c')), tmp.path());
        match action {
            Some(TuiAction::CancelTask { was_running, .. }) => {
                assert!(!was_running);
            }
            _ => panic!("Expected CancelTask action"),
        }
    }

    #[test]
    fn test_cancel_ignored_for_terminal_status() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_tasks();
        app.tasks[0].status = TaskStatus::Complete;
        app.focus = Panel::Tasks;

        let action = handle_event(&mut app, &make_key_event(KeyCode::Char('c')), tmp.path());
        assert!(action.is_none());
    }

    #[test]
    fn test_cancel_ignored_in_logs_panel() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_tasks();
        app.tasks[0].status = TaskStatus::Running;
        app.focus = Panel::Logs;

        let action = handle_event(&mut app, &make_key_event(KeyCode::Char('c')), tmp.path());
        assert!(action.is_none());
    }

    #[test]
    fn test_delete_completed_task() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_tasks();
        app.tasks[0].status = TaskStatus::Complete;
        app.focus = Panel::Tasks;

        let action = handle_event(&mut app, &make_key_event(KeyCode::Char('d')), tmp.path());
        match action {
            Some(TuiAction::DeleteTask { task_id, .. }) => {
                assert_eq!(task_id, "t1");
            }
            _ => panic!("Expected DeleteTask action"),
        }
    }

    #[test]
    fn test_delete_ignored_for_running_task() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_tasks();
        app.tasks[0].status = TaskStatus::Running;
        app.focus = Panel::Tasks;

        let action = handle_event(&mut app, &make_key_event(KeyCode::Char('d')), tmp.path());
        assert!(action.is_none());
    }

    #[test]
    fn test_delete_ignored_in_logs_panel() {
        let tmp = tempfile::tempdir().unwrap();
        let mut app = app_with_tasks();
        app.tasks[0].status = TaskStatus::Failed;
        app.focus = Panel::Logs;

        let action = handle_event(&mut app, &make_key_event(KeyCode::Char('d')), tmp.path());
        assert!(action.is_none());
    }
}
