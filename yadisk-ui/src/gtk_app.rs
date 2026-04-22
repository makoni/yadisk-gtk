use std::cell::{Cell, RefCell};
use std::collections::BTreeSet;
use std::process::Command;
use std::rc::Rc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use gtk4::{gio, glib, prelude::*};
use libadwaita::prelude::*;
use libadwaita::{ColorScheme, StyleManager};
use yadisk_integrations::i18n::{apply_language_preference, product_name, tr};
use yadisk_integrations::ids::APP_ID_GTK;
use yadisk_integrations::preferences::{LanguagePreference, load_ui_preferences};

use crate::control_client::ControlClient;
use crate::diagnostics::{diagnostics_report_json, export_support_bundle};
use crate::integration_control::{
    detect_integration_status, ensure_auto_install_permissions, guided_install_commands,
    run_auto_install, run_auto_uninstall,
};
use crate::service_control::{
    ServiceAction, auto_import_oauth_credentials, configure_oauth_credentials,
    oauth_credentials_configured, query_daemon_service_status, recent_daemon_journal,
    run_service_action,
};
use crate::ui_model::UiModel;

const ACTION_AUTH_START: &str = "action-auth-start";
const ACTION_AUTH_CANCEL: &str = "action-auth-cancel";
const ACTION_LOGOUT: &str = "action-logout";
const ACTION_DAEMON_START: &str = "action-daemon-start";
const ACTION_DAEMON_STOP: &str = "action-daemon-stop";
const ACTION_DAEMON_RESTART: &str = "action-daemon-restart";
const ACTION_INTEGRATIONS_CHECK: &str = "action-integrations-check";
const ACTION_INTEGRATIONS_GUIDED: &str = "action-integrations-guided";
const ACTION_INTEGRATIONS_AUTO: &str = "action-integrations-auto";
const ACTION_INTEGRATIONS_REMOVE: &str = "action-integrations-remove";
const ACTION_AUTOSTART_ENABLE: &str = "action-autostart-enable";
const ACTION_AUTOSTART_DISABLE: &str = "action-autostart-disable";
const ACTION_DIAGNOSTICS_DUMP: &str = "action-diagnostics-dump";
const ACTION_DIAGNOSTICS_EXPORT: &str = "action-diagnostics-export";

/// Run `work` on a background thread, then call `then` on the GTK main thread.
fn spawn_blocking<W, T, C>(work: W, then: C)
where
    W: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
    C: FnOnce(T) + 'static,
{
    // SAFETY: `then` is created on the main (GTK) thread. We convert it to a
    // raw pointer (as usize) so it can cross the thread boundary without
    // requiring Send. The background thread never dereferences the pointer —
    // it only forwards it to `glib::idle_add_once`, which runs the callback
    // on the main thread where the closure is reconstructed and called.
    let then_ptr = Box::into_raw(Box::new(then)) as usize;
    std::thread::spawn(move || {
        let result = work();
        glib::idle_add_once(move || {
            let f = unsafe { *Box::from_raw(then_ptr as *mut C) };
            f(result);
        });
    });
}

enum AuthStartOutcome {
    AuthUrl(String),
    CredentialsPrompt(String),
    ErrorDialog(String),
}

struct PostAuthOutcome {
    integration_needs_setup: bool,
    warning: Option<String>,
}

#[derive(Clone)]
enum TransientDialogState {
    AuthCode {
        auth_url: String,
        code: String,
    },
    OAuthCredentials {
        error_message: String,
        client_id: String,
        client_secret: String,
    },
}

thread_local! {
    static TRANSIENT_DIALOG_STATE: RefCell<Option<TransientDialogState>> = const { RefCell::new(None) };
}

fn set_transient_dialog_state(state: Option<TransientDialogState>) {
    TRANSIENT_DIALOG_STATE.with(|slot| {
        *slot.borrow_mut() = state;
    });
}

fn current_transient_dialog_state() -> Option<TransientDialogState> {
    TRANSIENT_DIALOG_STATE.with(|slot| slot.borrow().clone())
}

struct Widgets {
    overview_auth_badge: gtk4::Label,
    overview_daemon_badge: gtk4::Label,
    overview_integration_badge: gtk4::Label,
    auth_badge: gtk4::Label,
    account_label: gtk4::Label,
    daemon_badge: gtk4::Label,
    daemon_label: gtk4::Label,
    integration_badge: gtk4::Label,
    integration_label: gtk4::Label,
    integration_commands_box: gtk4::Box,
    settings_label: gtk4::Label,
    language_dropdown: gtk4::DropDown,
    diagnostics_label: gtk4::Label,
    auth_start_button: gtk4::Button,
    auth_cancel_button: gtk4::Button,
    auth_logout_button: gtk4::Button,
    autostart_enable_button: gtk4::Button,
    autostart_disable_button: gtk4::Button,
    integration_guided_commands: Rc<RefCell<Option<Vec<String>>>>,
}

pub fn run(start_tab: Option<String>) -> Result<()> {
    libadwaita::init()?;
    gtk4::Window::set_default_icon_name(APP_ID_GTK);
    install_css();
    apply_color_scheme_override_from_env();

    let app = libadwaita::Application::builder()
        .application_id(APP_ID_GTK)
        .build();

    app.connect_activate(move |app| present_or_build_window(app, start_tab.clone()));

    app.run_with_args::<&str>(&[]);
    Ok(())
}

fn apply_color_scheme_override_from_env() {
    let Ok(value) = std::env::var("YADISK_UI_FORCE_COLOR_SCHEME") else {
        return;
    };
    let scheme = match value.trim().to_ascii_lowercase().as_str() {
        "light" | "force-light" => ColorScheme::ForceLight,
        "dark" | "force-dark" => ColorScheme::ForceDark,
        "prefer-light" => ColorScheme::PreferLight,
        "prefer-dark" => ColorScheme::PreferDark,
        _ => return,
    };
    StyleManager::default().set_color_scheme(scheme);
}

fn present_or_build_window(app: &libadwaita::Application, start_tab: Option<String>) {
    if let Some(window) = app.windows().into_iter().next() {
        window.present();
        return;
    }
    build_window(app, start_tab);
}

fn build_window(app: &libadwaita::Application, start_tab: Option<String>) {
    let stack = gtk4::Stack::builder()
        .hexpand(true)
        .vexpand(true)
        .transition_type(gtk4::StackTransitionType::SlideLeftRight)
        .build();
    let sidebar = gtk4::StackSidebar::builder().stack(&stack).build();
    sidebar.add_css_class("navigation-sidebar");
    sidebar.set_vexpand(true);
    sidebar.set_width_request(182);
    stack.add_css_class("content-stack");

    let (overview_strip, overview_auth_badge, overview_daemon_badge, overview_integration_badge) =
        build_overview_strip();
    let widgets = Rc::new(build_pages(
        &stack,
        overview_auth_badge,
        overview_daemon_badge,
        overview_integration_badge,
    ));
    if let Some(start_tab) = start_tab.as_deref() {
        stack.set_visible_child_name(start_tab);
    }
    apply_model(&widgets, &UiModel::collect());

    let container = gtk4::Box::new(gtk4::Orientation::Horizontal, 16);
    container.set_margin_start(18);
    container.set_margin_end(18);
    container.set_margin_top(12);
    container.set_margin_bottom(18);
    container.append(&sidebar);
    container.append(&stack);

    let refresh_button = gtk4::Button::from_icon_name("view-refresh-symbolic");
    refresh_button.add_css_class("flat");
    refresh_button.set_tooltip_text(Some(tr("Refresh status").as_str()));
    let widgets_for_refresh = Rc::clone(&widgets);
    refresh_button.connect_clicked(move |_| refresh_ui_async(&widgets_for_refresh));

    let title_label = gtk4::Label::new(Some(product_name()));
    title_label.add_css_class("app-title");
    let header = libadwaita::HeaderBar::builder()
        .title_widget(&title_label)
        .build();
    header.add_css_class("app-header");
    header.pack_end(&refresh_button);

    let content = gtk4::Box::new(gtk4::Orientation::Vertical, 0);
    content.add_css_class("app-shell");
    content.append(&header);
    content.append(&overview_strip);
    content.append(&container);

    let window = libadwaita::ApplicationWindow::builder()
        .application(app)
        .title(product_name())
        .default_width(1060)
        .default_height(760)
        .content(&content)
        .build();
    window.add_css_class("app-window");

    {
        let widgets_for_tick = Rc::clone(&widgets);
        let refresh_pending = Rc::new(Cell::new(false));
        let weak_window = window.downgrade();
        glib::timeout_add_seconds_local(5, move || {
            if weak_window.upgrade().is_none() {
                return glib::ControlFlow::Break;
            }
            if !refresh_pending.get() {
                refresh_pending.set(true);
                let widgets = Rc::clone(&widgets_for_tick);
                let pending = Rc::clone(&refresh_pending);
                spawn_blocking(UiModel::collect, move |model| {
                    apply_model(&widgets, &model);
                    pending.set(false);
                });
            }
            glib::ControlFlow::Continue
        });
    }

    wire_actions(app, &stack, Rc::clone(&widgets), &window);
    restore_transient_dialog(&window, &stack, Rc::clone(&widgets));
    window.present();
}

fn build_pages(
    stack: &gtk4::Stack,
    overview_auth_badge: gtk4::Label,
    overview_daemon_badge: gtk4::Label,
    overview_integration_badge: gtk4::Label,
) -> Widgets {
    let (welcome, welcome_content) = page_shell();
    welcome_content.append(&page_heading("avatar-default-symbolic", "Welcome"));
    welcome_content.append(&page_description(
        "Connect your account and manage authorization from one place.",
    ));
    let auth_card = section_card();
    auth_card.add_css_class("brand-card");
    let auth_header = section_header("Authorization");
    let auth_badge = status_badge();
    auth_header.append(&auth_badge);
    auth_card.append(&auth_header);
    let account_label = body_label();
    auth_card.append(&account_label);
    let account_actions = action_row();
    let btn_auth_start = gtk4::Button::with_label(tr("Start Auth").as_str());
    btn_auth_start.set_widget_name(ACTION_AUTH_START);
    btn_auth_start.add_css_class("suggested-action");
    let btn_auth_cancel = gtk4::Button::with_label(tr("Cancel Auth").as_str());
    btn_auth_cancel.set_widget_name(ACTION_AUTH_CANCEL);
    let btn_logout = gtk4::Button::with_label(tr("Logout").as_str());
    btn_logout.set_widget_name(ACTION_LOGOUT);
    btn_logout.add_css_class("destructive-action");
    account_actions.append(&btn_auth_start);
    account_actions.append(&btn_auth_cancel);
    account_actions.append(&btn_logout);
    auth_card.append(&account_actions);
    welcome_content.append(&auth_card);
    welcome_content.append(&note_card(
        "Next step",
        "Start Auth, confirm access in the browser, then paste the code. After success the app automatically starts daemon, enables autostart, and checks integration readiness.",
    ));
    stack.add_titled(&welcome, Some("welcome"), tr("Welcome").as_str());

    let (sync, sync_content) = page_shell();
    sync_content.append(&page_heading("view-refresh-symbolic", "Sync"));
    sync_content.append(&page_description(
        "Manage the user service and check current daemon state.",
    ));
    let daemon_card = section_card();
    let daemon_header = section_header("Service status");
    let daemon_badge = status_badge();
    daemon_header.append(&daemon_badge);
    daemon_card.append(&daemon_header);
    let daemon_label = body_label();
    daemon_card.append(&daemon_label);
    let daemon_actions = action_row();
    let btn_start = gtk4::Button::with_label(tr("Start").as_str());
    btn_start.set_widget_name(ACTION_DAEMON_START);
    btn_start.add_css_class("suggested-action");
    let btn_stop = gtk4::Button::with_label(tr("Stop").as_str());
    btn_stop.set_widget_name(ACTION_DAEMON_STOP);
    btn_stop.add_css_class("destructive-action");
    let btn_restart = gtk4::Button::with_label(tr("Restart").as_str());
    btn_restart.set_widget_name(ACTION_DAEMON_RESTART);
    daemon_actions.append(&btn_start);
    daemon_actions.append(&btn_stop);
    daemon_actions.append(&btn_restart);
    daemon_card.append(&daemon_actions);
    sync_content.append(&daemon_card);
    sync_content.append(&note_card(
        "Service lifecycle",
        "Use Start when setting up for the first time, Restart after config changes, and Stop only for troubleshooting.",
    ));
    stack.add_titled(&sync, Some("sync"), tr("Sync Status").as_str());

    let (integrations, integrations_content) = page_shell();
    integrations_content.append(&page_heading("folder-symbolic", "Files Integration"));
    integrations_content.append(&page_description(
        "Check Nautilus/FUSE integration and run setup helpers.",
    ));
    let integrations_card = section_card();
    let integrations_header = section_header("Integration state");
    let integration_badge = status_badge();
    integrations_header.append(&integration_badge);
    integrations_card.append(&integrations_header);
    let integration_label = body_label();
    integrations_card.append(&integration_label);
    let integration_commands_box = gtk4::Box::new(gtk4::Orientation::Vertical, 8);
    integration_commands_box.set_visible(false);
    integrations_card.append(&integration_commands_box);
    let integration_actions = action_row();
    let btn_check = gtk4::Button::with_label(tr("Re-check").as_str());
    btn_check.set_widget_name(ACTION_INTEGRATIONS_CHECK);
    btn_check.add_css_class("suggested-action");
    let btn_guided = gtk4::Button::with_label(tr("Guided Install").as_str());
    btn_guided.set_widget_name(ACTION_INTEGRATIONS_GUIDED);
    let btn_auto = gtk4::Button::with_label(tr("Advanced Auto Install").as_str());
    btn_auto.set_widget_name(ACTION_INTEGRATIONS_AUTO);
    btn_auto.add_css_class("destructive-action");
    integration_actions.append(&btn_check);
    integration_actions.append(&btn_guided);
    integration_actions.append(&btn_auto);
    integrations_card.append(&integration_actions);
    integrations_content.append(&integrations_card);
    integrations_content.append(&note_card(
        "Recommendation",
        "Use Guided Install by default to follow safe host setup steps without elevated automatic actions.",
    ));
    let integration_remove_row = action_row();
    let btn_remove = gtk4::Button::with_label(tr("Remove integrations").as_str());
    btn_remove.set_widget_name(ACTION_INTEGRATIONS_REMOVE);
    btn_remove.add_css_class("destructive-action");
    integration_remove_row.append(&btn_remove);
    integrations_content.append(&integration_remove_row);
    stack.add_titled(
        &integrations,
        Some("integrations"),
        tr("Integrations").as_str(),
    );

    let (settings, settings_content) = page_shell();
    settings_content.append(&page_heading("emblem-system-symbolic", "Settings"));
    settings_content.append(&page_description(
        "Review service folders and startup behavior.",
    ));
    let settings_card = section_card();
    let language_label = section_title("Language");
    settings_card.append(&language_label);
    let language_dropdown = build_language_dropdown();
    settings_card.append(&language_dropdown);
    let settings_label = body_label();
    settings_label.add_css_class("monospace");
    settings_card.append(&settings_label);
    let settings_actions = action_row();
    let btn_auto_on = gtk4::Button::with_label(tr("Enable autostart").as_str());
    btn_auto_on.set_widget_name(ACTION_AUTOSTART_ENABLE);
    btn_auto_on.add_css_class("suggested-action");
    let btn_auto_off = gtk4::Button::with_label(tr("Disable autostart").as_str());
    btn_auto_off.set_widget_name(ACTION_AUTOSTART_DISABLE);
    btn_auto_off.add_css_class("destructive-action");
    settings_actions.append(&btn_auto_on);
    settings_actions.append(&btn_auto_off);
    settings_card.append(&settings_actions);
    settings_content.append(&settings_card);
    settings_content.append(&note_card(
        "Autostart",
        "Enable autostart after successful authorization so the sync daemon starts with your GNOME session.",
    ));
    stack.add_titled(&settings, Some("settings"), tr("Settings").as_str());

    let (diagnostics, diagnostics_content) = page_shell();
    diagnostics_content.append(&page_heading("utilities-terminal-symbolic", "Diagnostics"));
    diagnostics_content.append(&page_description(
        "Runtime status summary and diagnostics export helper.",
    ));
    let diagnostics_card = section_card();
    let diagnostics_label = body_label();
    diagnostics_label.add_css_class("monospace");
    diagnostics_card.append(&diagnostics_label);
    let diagnostics_actions = action_row();
    let btn_export = gtk4::Button::with_label(tr("Export support bundle").as_str());
    btn_export.set_widget_name(ACTION_DIAGNOSTICS_EXPORT);
    btn_export.add_css_class("suggested-action");
    diagnostics_actions.append(&btn_export);
    let btn_dump = gtk4::Button::with_label(tr("Show diagnostics").as_str());
    btn_dump.set_widget_name(ACTION_DIAGNOSTICS_DUMP);
    diagnostics_actions.append(&btn_dump);
    diagnostics_card.append(&diagnostics_actions);
    diagnostics_content.append(&diagnostics_card);
    diagnostics_content.append(&note_card(
        "Support tip",
        "Use Export support bundle when reporting issues so the current diagnostics snapshot and recent daemon logs are saved into one file.",
    ));
    stack.add_titled(
        &diagnostics,
        Some("diagnostics"),
        tr("Diagnostics").as_str(),
    );

    Widgets {
        overview_auth_badge,
        overview_daemon_badge,
        overview_integration_badge,
        auth_badge,
        account_label,
        daemon_badge,
        daemon_label,
        integration_badge,
        integration_label,
        integration_commands_box,
        settings_label,
        language_dropdown,
        diagnostics_label,
        auth_start_button: btn_auth_start,
        auth_cancel_button: btn_auth_cancel,
        auth_logout_button: btn_logout,
        autostart_enable_button: btn_auto_on,
        autostart_disable_button: btn_auto_off,
        integration_guided_commands: Rc::new(RefCell::new(None)),
    }
}

fn build_overview_strip() -> (gtk4::Box, gtk4::Label, gtk4::Label, gtk4::Label) {
    let strip = gtk4::Box::new(gtk4::Orientation::Horizontal, 10);
    strip.set_margin_start(20);
    strip.set_margin_end(20);
    strip.set_margin_bottom(8);
    strip.add_css_class("overview-strip");

    let (auth_tile, auth_badge) = overview_tile("Account");
    let (daemon_tile, daemon_badge) = overview_tile("Daemon");
    let (integration_tile, integration_badge) = overview_tile("Integrations");

    strip.append(&auth_tile);
    strip.append(&daemon_tile);
    strip.append(&integration_tile);
    (strip, auth_badge, daemon_badge, integration_badge)
}

fn overview_tile(title: &str) -> (gtk4::Box, gtk4::Label) {
    let tile = gtk4::Box::new(gtk4::Orientation::Horizontal, 8);
    tile.set_hexpand(true);
    tile.add_css_class("overview-tile");

    let title_label = gtk4::Label::new(Some(tr(title).as_str()));
    title_label.add_css_class("overview-title");
    title_label.set_halign(gtk4::Align::Start);
    title_label.set_hexpand(true);
    title_label.set_xalign(0.0);
    tile.append(&title_label);

    let badge = status_badge();
    tile.append(&badge);
    (tile, badge)
}

fn section_title(title: &str) -> gtk4::Label {
    let label = gtk4::Label::new(Some(tr(title).as_str()));
    label.add_css_class("title-3");
    label.set_halign(gtk4::Align::Start);
    label.set_xalign(0.0);
    label
}

fn page_box() -> gtk4::Box {
    let page = gtk4::Box::new(gtk4::Orientation::Vertical, 16);
    page.set_margin_start(6);
    page.set_margin_end(6);
    page.set_margin_top(6);
    page.set_margin_bottom(6);
    page.add_css_class("page-root");
    page
}

fn page_shell() -> (gtk4::Box, gtk4::Box) {
    let shell = gtk4::Box::new(gtk4::Orientation::Vertical, 0);
    let clamp = libadwaita::Clamp::builder()
        .maximum_size(920)
        .tightening_threshold(640)
        .build();
    let content = page_box();
    clamp.set_child(Some(&content));
    shell.append(&clamp);
    (shell, content)
}

fn section_header(title: &str) -> gtk4::Box {
    let row = gtk4::Box::new(gtk4::Orientation::Horizontal, 8);
    row.set_halign(gtk4::Align::Fill);
    row.set_hexpand(true);
    let title_label = section_title(title);
    title_label.set_hexpand(true);
    title_label.set_halign(gtk4::Align::Start);
    row.append(&title_label);
    row
}

fn page_heading(icon_name: &str, title: &str) -> gtk4::Box {
    let row = gtk4::Box::new(gtk4::Orientation::Horizontal, 8);
    row.add_css_class("page-hero");
    let icon = gtk4::Image::from_icon_name(icon_name);
    icon.set_pixel_size(20);
    icon.add_css_class("accent");
    let title_label = gtk4::Label::new(Some(tr(title).as_str()));
    title_label.add_css_class("title-2");
    title_label.add_css_class("page-heading");
    title_label.set_halign(gtk4::Align::Start);
    title_label.set_xalign(0.0);
    row.append(&icon);
    row.append(&title_label);
    row
}

fn page_description(text: &str) -> gtk4::Label {
    let label = gtk4::Label::new(Some(tr(text).as_str()));
    label.set_wrap(true);
    label.set_halign(gtk4::Align::Start);
    label.set_xalign(0.0);
    label.add_css_class("dim-label");
    label
}

fn section_card() -> gtk4::Box {
    let card = gtk4::Box::new(gtk4::Orientation::Vertical, 14);
    card.add_css_class("section-card");
    card
}

fn note_card(title: &str, text: &str) -> gtk4::Box {
    let card = section_card();
    card.add_css_class("section-muted");
    card.add_css_class("note-card");
    card.append(&section_title(title));
    let description = body_label();
    description.add_css_class("note-text");
    description.set_text(tr(text).as_str());
    card.append(&description);
    card
}

fn body_label() -> gtk4::Label {
    let label = gtk4::Label::new(None);
    label.set_wrap(true);
    label.set_selectable(false);
    label.set_halign(gtk4::Align::Start);
    label.set_xalign(0.0);
    label.add_css_class("body-copy");
    label
}

fn action_row() -> gtk4::Box {
    let row = gtk4::Box::new(gtk4::Orientation::Horizontal, 10);
    row.add_css_class("action-row");
    row
}

fn status_badge() -> gtk4::Label {
    let label = gtk4::Label::new(Some(tr("Unknown").as_str()));
    label.add_css_class("pill");
    label.add_css_class("status-unknown");
    label.set_halign(gtk4::Align::End);
    label
}

fn build_language_dropdown() -> gtk4::DropDown {
    let model = gtk4::StringList::new(&[]);
    for label in [tr("System default"), tr("English"), tr("Russian")] {
        model.append(&label);
    }
    let dropdown = gtk4::DropDown::builder().model(&model).build();
    dropdown.set_selected(language_preference_to_index(
        load_ui_preferences().language_preference,
    ));
    dropdown.set_hexpand(false);
    dropdown
}

fn language_preference_to_index(preference: LanguagePreference) -> u32 {
    match preference {
        LanguagePreference::System => 0,
        LanguagePreference::En => 1,
        LanguagePreference::Ru => 2,
    }
}

fn language_preference_from_index(index: u32) -> LanguagePreference {
    match index {
        1 => LanguagePreference::En,
        2 => LanguagePreference::Ru,
        _ => LanguagePreference::System,
    }
}

fn wire_actions(
    app: &libadwaita::Application,
    stack: &gtk4::Stack,
    widgets: Rc<Widgets>,
    window: &libadwaita::ApplicationWindow,
) {
    {
        let app = app.clone();
        let stack = stack.clone();
        let window = window.clone();
        let widgets = Rc::clone(&widgets);
        widgets
            .language_dropdown
            .connect_selected_notify(move |dropdown| {
                let preference = language_preference_from_index(dropdown.selected());
                if load_ui_preferences().language_preference == preference {
                    return;
                }
                match apply_language_preference(preference) {
                    Ok(true) => {
                        let current_tab = stack.visible_child_name().map(|name| name.to_string());
                        window.close();
                        build_window(&app, current_tab);
                    }
                    Ok(false) => {}
                    Err(err) => show_text_dialog(
                        &window,
                        tr("Language change failed").as_str(),
                        &format!("{}\n{err}", tr("Could not save the selected language.")),
                    ),
                }
            });
    }
    if let Some(button) = find_button(stack, ACTION_AUTH_START) {
        let widgets = Rc::clone(&widgets);
        let window = window.clone();
        let stack = stack.clone();
        button.connect_clicked(move |_| {
            let widgets = Rc::clone(&widgets);
            let window = window.clone();
            let stack = stack.clone();
            spawn_blocking(
                || -> AuthStartOutcome {
                    match start_auth_with_daemon_bootstrap() {
                        Ok(url) => AuthStartOutcome::AuthUrl(url),
                        Err(err) => {
                            let mut message = err.to_string();
                            if !oauth_credentials_configured()
                                && auto_import_oauth_credentials().unwrap_or(false)
                            {
                                match start_auth_with_daemon_bootstrap() {
                                    Ok(url) => return AuthStartOutcome::AuthUrl(url),
                                    Err(retry_err) => message = retry_err.to_string(),
                                }
                            }
                            if auth_env_missing_error(&message) || !oauth_credentials_configured() {
                                AuthStartOutcome::CredentialsPrompt(message)
                            } else {
                                AuthStartOutcome::ErrorDialog(message)
                            }
                        }
                    }
                },
                move |outcome| {
                    match outcome {
                        AuthStartOutcome::AuthUrl(url) => {
                            open_browser_url(&url);
                            prompt_auth_code_dialog(&window, &stack, Rc::clone(&widgets), url);
                        }
                        AuthStartOutcome::CredentialsPrompt(message) => {
                            prompt_oauth_credentials_dialog(
                                &window,
                                &stack,
                                Rc::clone(&widgets),
                                &message,
                            );
                        }
                        AuthStartOutcome::ErrorDialog(message) => {
                            show_text_dialog(
                                &window,
                                tr("Start Auth failed").as_str(),
                                &format!(
                                    "{}\n\n{}\n\n{}",
                                    tr("Could not start authorization."),
                                    message,
                                    tr("Check daemon status and try again.")
                                ),
                            );
                        }
                    }
                    refresh_ui_async(&widgets);
                },
            );
        });
    }
    if let Some(button) = find_button(stack, ACTION_AUTH_CANCEL) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            let widgets = Rc::clone(&widgets);
            spawn_blocking(
                || {
                    if let Ok(client) = ControlClient::connect() {
                        let _ = client.cancel_auth();
                    }
                },
                move |()| refresh_ui_async(&widgets),
            );
        });
    }
    if let Some(button) = find_button(stack, ACTION_LOGOUT) {
        let widgets = Rc::clone(&widgets);
        let window = window.clone();
        button.connect_clicked(move |_| {
            let dialog = libadwaita::MessageDialog::builder()
                .transient_for(&window)
                .heading(tr("Confirm logout").as_str())
                .body(tr("Remove saved credentials and disconnect this account?").as_str())
                .build();
            dialog.add_response("cancel", tr("Cancel").as_str());
            dialog.add_response("logout", tr("Logout").as_str());
            dialog.set_default_response(Some("cancel"));
            dialog.set_close_response("cancel");
            dialog.set_response_appearance("logout", libadwaita::ResponseAppearance::Destructive);
            let widgets_for_response = Rc::clone(&widgets);
            dialog.connect_response(None, move |dialog, response| {
                let do_logout = response == "logout";
                dialog.hide();
                let widgets = Rc::clone(&widgets_for_response);
                spawn_blocking(
                    move || {
                        if do_logout && let Ok(client) = ControlClient::connect() {
                            let _ = client.logout();
                        }
                    },
                    move |()| refresh_ui_async(&widgets),
                );
            });
            dialog.present();
        });
    }
    if let Some(button) = find_button(stack, ACTION_DAEMON_START) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            let widgets = Rc::clone(&widgets);
            spawn_blocking(
                || {
                    let _ = run_service_action(ServiceAction::Start);
                },
                move |()| refresh_ui_async(&widgets),
            );
        });
    }
    if let Some(button) = find_button(stack, ACTION_DAEMON_STOP) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            let widgets = Rc::clone(&widgets);
            spawn_blocking(
                || {
                    let _ = run_service_action(ServiceAction::Stop);
                },
                move |()| refresh_ui_async(&widgets),
            );
        });
    }
    if let Some(button) = find_button(stack, ACTION_DAEMON_RESTART) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            let widgets = Rc::clone(&widgets);
            spawn_blocking(
                || {
                    let _ = run_service_action(ServiceAction::Restart);
                },
                move |()| refresh_ui_async(&widgets),
            );
        });
    }
    if let Some(button) = find_button(stack, ACTION_INTEGRATIONS_CHECK) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            widgets.integration_guided_commands.borrow_mut().take();
            refresh_ui_async(&widgets);
        });
    }
    if let Some(button) = find_button(stack, ACTION_INTEGRATIONS_GUIDED) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            let commands = guided_install_commands();
            *widgets.integration_guided_commands.borrow_mut() = Some(commands.clone());
            widgets
                .integration_label
                .set_text(tr("Run these commands in a terminal, then press Re-check.").as_str());
            widgets.integration_label.set_selectable(false);
            render_guided_command_blocks(&widgets.integration_commands_box, &commands);
        });
    }
    if let Some(button) = find_button(stack, ACTION_INTEGRATIONS_AUTO) {
        let widgets = Rc::clone(&widgets);
        let window = window.clone();
        button.connect_clicked(move |_| {
            widgets.integration_guided_commands.borrow_mut().take();
            let widgets = Rc::clone(&widgets);
            let window = window.clone();
            spawn_blocking(
                || -> Result<(), String> {
                    ensure_auto_install_permissions().map_err(|e| format!("{}\n\n{e}", tr("Automatic install requires write access to the Nautilus extension directory or administrator privileges.")))?;
                    run_auto_install().map_err(|e| format!("{}\n{e}", tr("Could not install integration components:")))
                },
                move |result| {
                    match result {
                        Ok(()) => {
                            refresh_ui_async(&Rc::clone(&widgets));
                            prompt_nautilus_restart_dialog(
                                &window,
                                Rc::clone(&widgets),
                                tr("Restart GNOME Files (Nautilus) so it can load the new extension and refresh integration emblems.").as_str(),
                            );
                        }
                        Err(msg) => {
                            show_text_dialog(&window, tr("Auto install failed").as_str(), &msg);
                            refresh_ui_async(&widgets);
                        }
                    }
                },
            );
        });
    }
    if let Some(button) = find_button(stack, ACTION_INTEGRATIONS_REMOVE) {
        let widgets = Rc::clone(&widgets);
        let window = window.clone();
        button.connect_clicked(move |_| {
            let dialog = libadwaita::MessageDialog::builder()
                .transient_for(&window)
                .heading(tr("Confirm integration removal").as_str())
                .body(
                    tr("Remove Nautilus extension, FUSE helper, and installed emblem icons?")
                        .as_str(),
                )
                .build();
            dialog.add_response("cancel", tr("Cancel").as_str());
            dialog.add_response("remove", tr("Remove").as_str());
            dialog.set_default_response(Some("cancel"));
            dialog.set_close_response("cancel");
            dialog.set_response_appearance("remove", libadwaita::ResponseAppearance::Destructive);
            let widgets_for_response = Rc::clone(&widgets);
            let window_for_response = window.clone();
            dialog.connect_response(None, move |dialog, response| {
                let do_remove = response == "remove";
                if do_remove {
                    widgets_for_response
                        .integration_guided_commands
                        .borrow_mut()
                        .take();
                }
                dialog.hide();
                let widgets = Rc::clone(&widgets_for_response);
                let window = window_for_response.clone();
                spawn_blocking(
                    move || {
                        if do_remove { run_auto_uninstall().err() } else { None }
                    },
                    move |uninstall_err| {
                        if let Some(err) = uninstall_err {
                            show_text_dialog(
                                &window,
                                tr("Remove integrations failed").as_str(),
                                &format!(
                                    "{}\n{err}\n\n{}",
                                    tr("Could not remove all integration files:"),
                                    tr("Some system files may require administrator privileges.")
                                ),
                            );
                            refresh_ui_async(&widgets);
                        } else if do_remove {
                            refresh_ui_async(&Rc::clone(&widgets));
                            prompt_nautilus_restart_dialog(
                                &window,
                                Rc::clone(&widgets),
                                tr("Restart GNOME Files (Nautilus) so it unloads removed integration components and refreshes overlays.").as_str(),
                            );
                        } else {
                            refresh_ui_async(&widgets);
                        }
                    },
                );
            });
            dialog.present();
        });
    }
    if let Some(button) = find_button(stack, ACTION_AUTOSTART_ENABLE) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            let widgets = Rc::clone(&widgets);
            spawn_blocking(
                || {
                    let _ = run_service_action(ServiceAction::EnableAutostart);
                },
                move |()| refresh_ui_async(&widgets),
            );
        });
    }
    if let Some(button) = find_button(stack, ACTION_AUTOSTART_DISABLE) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            let widgets = Rc::clone(&widgets);
            spawn_blocking(
                || {
                    let _ = run_service_action(ServiceAction::DisableAutostart);
                },
                move |()| refresh_ui_async(&widgets),
            );
        });
    }
    if let Some(button) = find_button(stack, ACTION_DIAGNOSTICS_DUMP) {
        let widgets = Rc::clone(&widgets);
        let window = window.clone();
        button.connect_clicked(move |_| {
            let widgets = Rc::clone(&widgets);
            let window = window.clone();
            spawn_blocking(
                || {
                    let model = UiModel::collect();
                    match diagnostics_report_json(
                        model.control.as_ref(),
                        model.service.as_ref(),
                        &model.integrations,
                        model.settings.clone(),
                    ) {
                        Ok(json) => Ok(json),
                        Err(err) => Err(format!(
                            "{}\n{err}",
                            tr("Failed to build diagnostics report:")
                        )),
                    }
                },
                move |result| {
                    match result {
                        Ok(json) => show_text_dialog(&window, tr("Diagnostics").as_str(), &json),
                        Err(msg) => {
                            show_text_dialog(&window, tr("Diagnostics error").as_str(), &msg)
                        }
                    }
                    refresh_ui_async(&widgets);
                },
            );
        });
    }
    if let Some(button) = find_button(stack, ACTION_DIAGNOSTICS_EXPORT) {
        let widgets = Rc::clone(&widgets);
        let window = window.clone();
        button.connect_clicked(move |_| {
            let widgets = Rc::clone(&widgets);
            let window = window.clone();
            spawn_blocking(
                || -> std::result::Result<std::path::PathBuf, String> {
                    let model = UiModel::collect();
                    let logs = recent_daemon_journal(400).map_err(|err| {
                        format!("{}\n{err}", tr("Failed to collect daemon logs:"))
                    })?;
                    export_support_bundle(
                        model.control.as_ref(),
                        model.service.as_ref(),
                        &model.integrations,
                        model.settings.clone(),
                        &logs,
                    )
                    .map_err(|err| {
                        format!("{}\n{err}", tr("Failed to export support bundle:"))
                    })
                },
                move |result| {
                    match result {
                        Ok(path) => show_text_dialog(
                            &window,
                            tr("Support bundle exported").as_str(),
                            &format!(
                                "{}\n{}\n\n{}\n\n{}",
                                tr("Saved support bundle to:"),
                                path.display(),
                                tr("The file includes diagnostics state and recent daemon logs."),
                                tr("Review it before sharing because file paths and operation names may be visible.")
                            ),
                        ),
                        Err(msg) => show_text_dialog(
                            &window,
                            tr("Support bundle export failed").as_str(),
                            &msg,
                        ),
                    }
                    refresh_ui_async(&widgets);
                },
            );
        });
    }
}

fn find_button(root: &gtk4::Stack, widget_name: &str) -> Option<gtk4::Button> {
    fn search(widget: &gtk4::Widget, widget_name: &str) -> Option<gtk4::Button> {
        if widget.widget_name() == widget_name {
            return widget.clone().downcast::<gtk4::Button>().ok();
        }
        let mut child = widget.first_child();
        while let Some(current) = child {
            if let Some(found) = search(&current, widget_name) {
                return Some(found);
            }
            child = current.next_sibling();
        }
        None
    }
    search(root.as_ref(), widget_name)
}

fn start_auth_with_daemon_bootstrap() -> Result<String> {
    let first_error = match start_auth_once() {
        Ok(url) => return Ok(url),
        Err(err) => err,
    };
    let _ = run_service_action(ServiceAction::Restart);
    match start_auth_once() {
        Ok(url) => Ok(url),
        Err(retry_error) => Err(anyhow::anyhow!(
            "initial attempt failed: {first_error}; retry after daemon restart failed: {retry_error}"
        )),
    }
}

fn start_auth_once() -> Result<String> {
    if let Ok(client) = ControlClient::connect() {
        return client.start_auth();
    }
    run_service_action(ServiceAction::Start)?;
    let client = ControlClient::connect()?;
    client.start_auth()
}

fn submit_auth_code_with_daemon_bootstrap(code: &str) -> Result<()> {
    if let Ok(client) = ControlClient::connect() {
        return client.submit_auth_code(code);
    }
    run_service_action(ServiceAction::Start)?;
    let client = ControlClient::connect()?;
    client.submit_auth_code(code)
}

fn open_browser_url(url: &str) {
    if gio::AppInfo::launch_default_for_uri(url, None::<&gio::AppLaunchContext>).is_err() {
        let _ = std::process::Command::new("xdg-open").arg(url).spawn();
    }
}

fn restore_transient_dialog(
    window: &libadwaita::ApplicationWindow,
    stack: &gtk4::Stack,
    widgets: Rc<Widgets>,
) {
    match current_transient_dialog_state() {
        Some(TransientDialogState::AuthCode { auth_url, code }) => {
            prompt_auth_code_dialog_with_initial(window, stack, widgets, auth_url, code);
        }
        Some(TransientDialogState::OAuthCredentials {
            error_message,
            client_id,
            client_secret,
        }) => {
            prompt_oauth_credentials_dialog_with_initial(
                window,
                stack,
                widgets,
                &error_message,
                &client_id,
                &client_secret,
            );
        }
        None => {}
    }
}

fn prompt_auth_code_dialog(
    window: &libadwaita::ApplicationWindow,
    stack: &gtk4::Stack,
    widgets: Rc<Widgets>,
    auth_url: String,
) {
    prompt_auth_code_dialog_with_initial(window, stack, widgets, auth_url, String::new());
}

fn prompt_auth_code_dialog_with_initial(
    window: &libadwaita::ApplicationWindow,
    stack: &gtk4::Stack,
    widgets: Rc<Widgets>,
    auth_url: String,
    initial_code: String,
) {
    let dialog = gtk4::Dialog::builder()
        .title(tr("Finish authorization").as_str())
        .modal(true)
        .transient_for(window)
        .build();
    dialog.add_button(
        tr("Open browser again").as_str(),
        gtk4::ResponseType::Other(1),
    );
    dialog.add_button(tr("Cancel").as_str(), gtk4::ResponseType::Cancel);
    dialog.add_button(tr("Submit code").as_str(), gtk4::ResponseType::Accept);
    dialog.set_default_response(gtk4::ResponseType::Accept);

    let content = dialog.content_area();
    let body = gtk4::Box::new(gtk4::Orientation::Vertical, 8);
    let message = gtk4::Label::new(Some(
        tr("Authorize access in your browser, then paste the verification code here.").as_str(),
    ));
    message.set_wrap(true);
    message.set_halign(gtk4::Align::Start);
    message.set_xalign(0.0);
    let entry = gtk4::Entry::new();
    entry.set_placeholder_text(Some(tr("Paste verification code").as_str()));
    entry.set_activates_default(true);
    entry.set_text(&initial_code);
    body.append(&message);
    body.append(&entry);
    content.append(&body);

    set_transient_dialog_state(Some(TransientDialogState::AuthCode {
        auth_url: auth_url.clone(),
        code: initial_code,
    }));

    let entry_for_response = entry.clone();
    let auth_url_for_response = auth_url.clone();
    let stack_for_response = stack.clone();
    let window_for_response = window.clone();
    entry.connect_changed({
        let auth_url = auth_url.clone();
        move |entry| {
            set_transient_dialog_state(Some(TransientDialogState::AuthCode {
                auth_url: auth_url.clone(),
                code: entry.text().to_string(),
            }));
        }
    });
    dialog.connect_response(move |dialog, response| match response {
        gtk4::ResponseType::Accept => {
            let code = entry_for_response.text().trim().to_string();
            dialog.close();
            set_transient_dialog_state(None);
            if !code.is_empty() {
                let w = Rc::clone(&widgets);
                let stack = stack_for_response.clone();
                let window = window_for_response.clone();
                spawn_blocking(
                    move || -> Result<PostAuthOutcome, String> {
                        submit_auth_code_with_daemon_bootstrap(&code).map_err(|e| e.to_string())?;
                        Ok(run_post_auth_steps())
                    },
                    move |result| {
                        match result {
                            Ok(outcome) => {
                                if outcome.integration_needs_setup {
                                    stack.set_visible_child_name("integrations");
                                } else {
                                    stack.set_visible_child_name("sync");
                                }
                                if let Some(warning) = outcome.warning {
                                    show_text_dialog(
                                        &window,
                                        tr("Authorization completed with warnings").as_str(),
                                        &warning,
                                    );
                                }
                            }
                            Err(msg) => {
                                show_text_dialog(
                                    &window,
                                    tr("Authorization failed").as_str(),
                                    &format!(
                                        "{}\n{msg}",
                                        tr("Failed to submit verification code:")
                                    ),
                                );
                            }
                        }
                        refresh_ui_async(&w);
                    },
                );
            } else {
                refresh_ui_async(&widgets);
            }
        }
        gtk4::ResponseType::Other(1) => {
            open_browser_url(&auth_url_for_response);
        }
        _ => {
            dialog.close();
            set_transient_dialog_state(None);
            refresh_ui_async(&widgets);
        }
    });
    dialog.present();
}

fn run_post_auth_steps() -> PostAuthOutcome {
    let mut warnings = Vec::new();
    if let Err(err) = run_service_action(ServiceAction::Restart)
        .or_else(|_| run_service_action(ServiceAction::Start))
    {
        warnings.push(format!(
            "{}\n{err}",
            tr("Could not restart or start the sync service:")
        ));
    }
    match query_daemon_service_status() {
        Ok(status) if matches!(status.normalized(), "active" | "activating" | "reloading") => {}
        Ok(status) => warnings.push(format!(
            "{}\n{}",
            tr("The sync service is not active yet."),
            status.normalized()
        )),
        Err(err) => warnings.push(format!(
            "{}\n{err}",
            tr("Could not verify sync service status:")
        )),
    }
    if let Err(err) = run_service_action(ServiceAction::EnableAutostart) {
        warnings.push(format!("{}\n{err}", tr("Could not enable autostart:")));
    }
    let model = UiModel::collect();
    PostAuthOutcome {
        integration_needs_setup: !matches!(
            model.integration_status,
            crate::ui_model::UiStatus::Ready
        ),
        warning: (!warnings.is_empty()).then(|| {
            format!(
                "{}\n\n{}",
                tr("Authorization succeeded, but some follow-up steps need attention:"),
                warnings.join("\n\n")
            )
        }),
    }
}

fn show_text_dialog(window: &libadwaita::ApplicationWindow, title: &str, text: &str) {
    let dialog = libadwaita::MessageDialog::builder()
        .transient_for(window)
        .heading(title)
        .body(text)
        .build();
    dialog.add_response("ok", tr("OK").as_str());
    dialog.set_default_response(Some("ok"));
    dialog.set_close_response("ok");
    dialog.connect_response(Some("ok"), |dialog, _| {
        dialog.hide();
    });
    dialog.present();
}

fn prompt_nautilus_restart_dialog(
    window: &libadwaita::ApplicationWindow,
    widgets: Rc<Widgets>,
    reason: &str,
) {
    let dialog = libadwaita::MessageDialog::builder()
        .transient_for(window)
        .heading(tr("Restart Files now?").as_str())
        .body(reason)
        .build();
    dialog.add_response("later", tr("Not now").as_str());
    dialog.add_response("restart", tr("Restart Files").as_str());
    dialog.set_default_response(Some("later"));
    dialog.set_close_response("later");
    dialog.set_response_appearance("restart", libadwaita::ResponseAppearance::Suggested);
    let window_for_response = window.clone();
    dialog.connect_response(None, move |dialog, response| {
        let do_restart = response == "restart";
        dialog.hide();
        let w = Rc::clone(&widgets);
        let window = window_for_response.clone();
        spawn_blocking(
            move || {
                if do_restart {
                    restart_nautilus()
                        .err()
                        .map(|e| format!("{}\n{e}", tr("Could not restart Nautilus:")))
                } else {
                    None
                }
            },
            move |err_msg| {
                if let Some(msg) = err_msg {
                    show_text_dialog(&window, tr("Restart Files failed").as_str(), &msg);
                }
                refresh_ui_async(&w);
            },
        );
    });
    dialog.present();
}

fn restart_nautilus() -> Result<()> {
    let integration_status = detect_integration_status();
    let before = nautilus_pids();
    let quit_status = Command::new("nautilus")
        .arg("-q")
        .status()
        .context("failed to run nautilus -q")?;
    if !quit_status.success() && quit_status.code() != Some(255) {
        anyhow::bail!("nautilus -q exited with status {quit_status}");
    }
    wait_for_nautilus_shutdown(&before)?;
    let relaunch_status = Command::new("nautilus")
        .arg("--new-window")
        .status()
        .context("failed to relaunch Nautilus after quit")?;
    if !relaunch_status.success() {
        anyhow::bail!("nautilus --new-window exited with status {relaunch_status}");
    }
    let pid = wait_for_nautilus_restart(&before)?;
    if integration_status.nautilus_extension_installed {
        verify_nautilus_extension_loaded(pid)?;
    }
    Ok(())
}

fn nautilus_pids() -> BTreeSet<u32> {
    nautilus_pids_in(std::path::Path::new("/proc"))
}

fn nautilus_pids_in(proc_root: &std::path::Path) -> BTreeSet<u32> {
    let mut pids = BTreeSet::new();
    let Ok(entries) = std::fs::read_dir(proc_root) else {
        return pids;
    };
    for entry in entries.flatten() {
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        let Ok(pid) = name.parse::<u32>() else {
            continue;
        };
        let proc_dir = entry.path();
        let Ok(comm) = std::fs::read_to_string(proc_dir.join("comm")) else {
            continue;
        };
        if comm.trim() == "nautilus" && process_state(&proc_dir) != Some('Z') {
            pids.insert(pid);
        }
    }
    pids
}

fn process_state(proc_dir: &std::path::Path) -> Option<char> {
    let status = std::fs::read_to_string(proc_dir.join("status")).ok()?;
    parse_process_status_state(&status)
}

fn parse_process_status_state(status: &str) -> Option<char> {
    status
        .lines()
        .find_map(|line| line.strip_prefix("State:"))
        .and_then(|value| value.trim_start().chars().next())
}

fn wait_for_nautilus_shutdown(previous: &BTreeSet<u32>) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let current = nautilus_pids();
        if previous.is_disjoint(&current) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    anyhow::bail!("timed out while waiting for Nautilus to exit");
}

fn wait_for_nautilus_restart(previous: &BTreeSet<u32>) -> Result<u32> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let current = nautilus_pids();
        if let Some(pid) = current.into_iter().find(|pid| !previous.contains(pid)) {
            return Ok(pid);
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    anyhow::bail!("timed out while waiting for Nautilus to relaunch");
}

fn verify_nautilus_extension_loaded(pid: u32) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let maps_path = format!("/proc/{pid}/maps");
        if let Ok(maps) = std::fs::read_to_string(&maps_path)
            && maps.contains("libyadisk_nautilus.so")
        {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    anyhow::bail!("Nautilus restarted, but libyadisk_nautilus.so was not loaded");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_process_status_state() {
        assert_eq!(
            parse_process_status_state("Name:\tnautilus\nState:\tZ (zombie)\nPid:\t42\n"),
            Some('Z')
        );
        assert_eq!(
            parse_process_status_state("Name:\tnautilus\nState:\tS (sleeping)\nPid:\t42\n"),
            Some('S')
        );
        assert_eq!(
            parse_process_status_state("Name:\tnautilus\nPid:\t42\n"),
            None
        );
    }

    #[test]
    fn nautilus_pids_ignore_zombies() {
        let temp = unique_test_dir();
        write_fake_proc_entry(
            &temp,
            100,
            "nautilus\n",
            "Name:\tnautilus\nState:\tZ (zombie)\n",
        );
        write_fake_proc_entry(
            &temp,
            101,
            "nautilus\n",
            "Name:\tnautilus\nState:\tS (sleeping)\n",
        );
        write_fake_proc_entry(&temp, 102, "bash\n", "Name:\tbash\nState:\tS (sleeping)\n");

        assert_eq!(nautilus_pids_in(&temp), BTreeSet::from([101]));
        let _ = std::fs::remove_dir_all(&temp);
    }

    fn write_fake_proc_entry(root: &std::path::Path, pid: u32, comm: &str, status: &str) {
        let dir = root.join(pid.to_string());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("comm"), comm).unwrap();
        std::fs::write(dir.join("status"), status).unwrap();
    }

    fn unique_test_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "yadisk-ui-nautilus-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}

fn auth_env_missing_error(err: &str) -> bool {
    err.contains("YADISK_CLIENT_ID is missing")
        || err.contains("YADISK_CLIENT_SECRET is missing")
        || err.contains("YADISK_CLIENT_ID is not set")
        || err.contains("YADISK_CLIENT_SECRET is not set")
}

fn prompt_oauth_credentials_dialog(
    window: &libadwaita::ApplicationWindow,
    stack: &gtk4::Stack,
    widgets: Rc<Widgets>,
    error_message: &str,
) {
    prompt_oauth_credentials_dialog_with_initial(window, stack, widgets, error_message, "", "");
}

fn prompt_oauth_credentials_dialog_with_initial(
    window: &libadwaita::ApplicationWindow,
    stack: &gtk4::Stack,
    widgets: Rc<Widgets>,
    error_message: &str,
    initial_client_id: &str,
    initial_client_secret: &str,
) {
    let dialog = gtk4::Dialog::builder()
        .title(tr("OAuth credentials required").as_str())
        .modal(true)
        .transient_for(window)
        .build();
    dialog.add_button(tr("Cancel").as_str(), gtk4::ResponseType::Cancel);
    dialog.add_button(tr("Save and retry").as_str(), gtk4::ResponseType::Accept);
    dialog.set_default_response(gtk4::ResponseType::Accept);
    let content = dialog.content_area();
    let body = gtk4::Box::new(gtk4::Orientation::Vertical, 8);
    let message = gtk4::Label::new(Some(&format!(
        "{}\n\n{}\n\n{}",
        tr("Authorization cannot start because OAuth credentials are missing in daemon service."),
        error_message,
        tr("Enter Yandex OAuth client id and secret:")
    )));
    message.set_wrap(true);
    message.set_halign(gtk4::Align::Start);
    message.set_xalign(0.0);
    let client_id = gtk4::Entry::new();
    client_id.set_placeholder_text(Some("YADISK_CLIENT_ID"));
    client_id.set_text(initial_client_id);
    let client_secret = gtk4::Entry::new();
    client_secret.set_placeholder_text(Some("YADISK_CLIENT_SECRET"));
    client_secret.set_visibility(false);
    client_secret.set_text(initial_client_secret);
    body.append(&message);
    body.append(&client_id);
    body.append(&client_secret);
    content.append(&body);

    set_transient_dialog_state(Some(TransientDialogState::OAuthCredentials {
        error_message: error_message.to_string(),
        client_id: initial_client_id.to_string(),
        client_secret: initial_client_secret.to_string(),
    }));

    let client_id_for_response = client_id.clone();
    let client_secret_for_response = client_secret.clone();
    let window_for_response = window.clone();
    let stack_for_response = stack.clone();
    client_id.connect_changed({
        let error_message = error_message.to_string();
        let client_secret = client_secret.clone();
        move |entry| {
            set_transient_dialog_state(Some(TransientDialogState::OAuthCredentials {
                error_message: error_message.clone(),
                client_id: entry.text().to_string(),
                client_secret: client_secret.text().to_string(),
            }));
        }
    });
    client_secret.connect_changed({
        let error_message = error_message.to_string();
        let client_id = client_id.clone();
        move |entry| {
            set_transient_dialog_state(Some(TransientDialogState::OAuthCredentials {
                error_message: error_message.clone(),
                client_id: client_id.text().to_string(),
                client_secret: entry.text().to_string(),
            }));
        }
    });
    dialog.connect_response(move |dialog, response| {
        if response == gtk4::ResponseType::Accept {
            let id = client_id_for_response.text().trim().to_string();
            let secret = client_secret_for_response.text().trim().to_string();
            dialog.close();
            set_transient_dialog_state(None);
            let w = Rc::clone(&widgets);
            let window = window_for_response.clone();
            let stack = stack_for_response.clone();
            spawn_blocking(
                move || -> Result<String, String> {
                    configure_oauth_credentials(&id, &secret)
                        .map_err(|e| format!("{}\n{e}", tr("Could not save OAuth credentials:")))?;
                    start_auth_with_daemon_bootstrap().map_err(|e| {
                        format!(
                            "{}\n{e}",
                            tr("Could not start authorization after saving credentials:")
                        )
                    })
                },
                move |result| {
                    match result {
                        Ok(url) => {
                            open_browser_url(&url);
                            prompt_auth_code_dialog(&window, &stack, Rc::clone(&w), url);
                        }
                        Err(msg) => {
                            show_text_dialog(&window, tr("Auth error").as_str(), &msg);
                        }
                    }
                    refresh_ui_async(&w);
                },
            );
        } else {
            dialog.close();
            set_transient_dialog_state(None);
        }
    });
    dialog.present();
}

fn apply_model(widgets: &Widgets, model: &UiModel) {
    update_badge(&widgets.overview_auth_badge, model.auth_status);
    update_badge(&widgets.overview_daemon_badge, model.daemon_status);
    update_badge(
        &widgets.overview_integration_badge,
        model.integration_status,
    );
    update_badge(&widgets.auth_badge, model.auth_status);
    update_badge(&widgets.daemon_badge, model.daemon_status);
    update_badge(&widgets.integration_badge, model.integration_status);
    widgets.account_label.set_text(&model.auth_summary);
    widgets.daemon_label.set_text(&model.daemon_summary);
    if let Some(commands) = widgets.integration_guided_commands.borrow().as_ref() {
        widgets
            .integration_label
            .set_text(tr("Run these commands in a terminal, then press Re-check.").as_str());
        widgets.integration_label.set_selectable(false);
        if widgets.integration_commands_box.first_child().is_none() {
            render_guided_command_blocks(&widgets.integration_commands_box, commands);
        } else {
            widgets.integration_commands_box.set_visible(true);
        }
    } else {
        widgets
            .integration_label
            .set_text(&model.integration_summary);
        widgets.integration_label.set_selectable(false);
        if widgets.integration_commands_box.first_child().is_some() {
            render_guided_command_blocks(&widgets.integration_commands_box, &[]);
        } else {
            widgets.integration_commands_box.set_visible(false);
        }
    }
    widgets.settings_label.set_text(&format!(
        "{}: {}\n{}: {}\n{}: {}\n{}: {}\n{}: {}\n{}: {}\n{}: {}",
        tr("Sync folder"),
        model.settings.sync_root,
        tr("Cache folder"),
        model.settings.cache_root,
        tr("Remote root"),
        model.settings.remote_root,
        tr("Cloud poll interval (s)"),
        model.settings.cloud_poll_secs,
        tr("Worker loop (ms)"),
        model.settings.worker_loop_ms,
        tr("Local watcher enabled"),
        localized_bool(model.settings.local_watcher_enabled),
        tr("Autostart"),
        localize_autostart_state(model.settings.autostart.as_str())
    ));
    widgets.diagnostics_label.set_text(&format!(
        "{}: {}\n{}: {}\n{}: {}",
        tr("Daemon status"),
        localized_ui_status(model.daemon_status),
        tr("Auth status"),
        localized_ui_status(model.auth_status),
        tr("Integrations status"),
        localized_ui_status(model.integration_status)
    ));
    widgets.settings_label.set_selectable(false);
    widgets.diagnostics_label.set_selectable(false);
    apply_action_visibility(widgets, model);
}

fn refresh_ui_async(widgets: &Rc<Widgets>) {
    let widgets = Rc::clone(widgets);
    spawn_blocking(UiModel::collect, move |model| {
        apply_model(&widgets, &model);
    });
}

fn localized_bool(value: bool) -> String {
    if value { tr("Yes") } else { tr("No") }
}

fn localize_autostart_state(state: &str) -> String {
    match state {
        "enabled" | "enabled-runtime" | "linked" | "linked-runtime" => tr("Enabled"),
        "disabled" => tr("Disabled"),
        "masked" => tr("Masked"),
        "unknown" => tr("Unknown"),
        other => other.to_string(),
    }
}

fn localized_ui_status(status: crate::ui_model::UiStatus) -> String {
    match status {
        crate::ui_model::UiStatus::Ready => tr("Ready"),
        crate::ui_model::UiStatus::NeedsSetup => tr("Needs setup"),
        crate::ui_model::UiStatus::Error => tr("Error"),
        crate::ui_model::UiStatus::Unknown => tr("Unknown"),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthActionState {
    Unauthorized,
    Authorizing,
    Authorized,
    Error,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AutostartActionState {
    Enabled,
    Disabled,
    Unknown,
}

fn auth_action_state(model: &UiModel) -> AuthActionState {
    if let Some(control) = &model.control {
        return match control.auth_state.as_str() {
            "authorized" => AuthActionState::Authorized,
            "pending" => AuthActionState::Authorizing,
            "unauthorized" | "cancelled" => AuthActionState::Unauthorized,
            "error" => AuthActionState::Error,
            _ => AuthActionState::Unknown,
        };
    }
    match model.auth_status {
        crate::ui_model::UiStatus::Ready => AuthActionState::Authorized,
        crate::ui_model::UiStatus::NeedsSetup => AuthActionState::Unauthorized,
        crate::ui_model::UiStatus::Error => AuthActionState::Error,
        crate::ui_model::UiStatus::Unknown => AuthActionState::Unknown,
    }
}

fn autostart_action_state(autostart: &str) -> AutostartActionState {
    match autostart {
        "enabled" | "enabled-runtime" | "linked" | "linked-runtime" => {
            AutostartActionState::Enabled
        }
        "disabled" | "masked" => AutostartActionState::Disabled,
        _ => AutostartActionState::Unknown,
    }
}

fn apply_action_visibility(widgets: &Widgets, model: &UiModel) {
    match auth_action_state(model) {
        AuthActionState::Unauthorized => {
            widgets.auth_start_button.set_visible(true);
            widgets.auth_cancel_button.set_visible(false);
            widgets.auth_logout_button.set_visible(false);
        }
        AuthActionState::Authorizing => {
            widgets.auth_start_button.set_visible(false);
            widgets.auth_cancel_button.set_visible(true);
            widgets.auth_logout_button.set_visible(false);
        }
        AuthActionState::Authorized => {
            widgets.auth_start_button.set_visible(false);
            widgets.auth_cancel_button.set_visible(false);
            widgets.auth_logout_button.set_visible(true);
        }
        AuthActionState::Error => {
            widgets.auth_start_button.set_visible(true);
            widgets.auth_cancel_button.set_visible(false);
            widgets.auth_logout_button.set_visible(false);
        }
        AuthActionState::Unknown => {
            widgets.auth_start_button.set_visible(true);
            widgets.auth_cancel_button.set_visible(true);
            widgets.auth_logout_button.set_visible(true);
        }
    }

    match autostart_action_state(model.settings.autostart.as_str()) {
        AutostartActionState::Enabled => {
            widgets.autostart_enable_button.set_visible(false);
            widgets.autostart_disable_button.set_visible(true);
        }
        AutostartActionState::Disabled => {
            widgets.autostart_enable_button.set_visible(true);
            widgets.autostart_disable_button.set_visible(false);
        }
        AutostartActionState::Unknown => {
            widgets.autostart_enable_button.set_visible(true);
            widgets.autostart_disable_button.set_visible(true);
        }
    }
}

fn update_badge(label: &gtk4::Label, status: crate::ui_model::UiStatus) {
    label.remove_css_class("status-ready");
    label.remove_css_class("status-needs");
    label.remove_css_class("status-error");
    label.remove_css_class("status-unknown");
    match status {
        crate::ui_model::UiStatus::Ready => {
            label.set_text(tr("Ready").as_str());
            label.add_css_class("status-ready");
        }
        crate::ui_model::UiStatus::NeedsSetup => {
            label.set_text(tr("Needs setup").as_str());
            label.add_css_class("status-needs");
        }
        crate::ui_model::UiStatus::Error => {
            label.set_text(tr("Error").as_str());
            label.add_css_class("status-error");
        }
        crate::ui_model::UiStatus::Unknown => {
            label.set_text(tr("Unknown").as_str());
            label.add_css_class("status-unknown");
        }
    }
}

fn render_guided_command_blocks(container: &gtk4::Box, commands: &[String]) {
    while let Some(child) = container.first_child() {
        container.remove(&child);
    }
    container.set_visible(!commands.is_empty());
    for (index, command) in commands.iter().enumerate() {
        let step_title = gtk4::Label::new(Some(&format!(
            "{}. {}",
            index + 1,
            guided_step_title(index)
        )));
        step_title.set_halign(gtk4::Align::Start);
        step_title.set_xalign(0.0);
        step_title.set_wrap(true);
        step_title.add_css_class("body-copy");
        container.append(&step_title);

        let row = gtk4::Box::new(gtk4::Orientation::Horizontal, 8);
        row.add_css_class("section-muted");
        row.set_spacing(12);
        row.set_margin_top(2);
        row.set_margin_bottom(8);
        row.set_margin_start(8);
        row.set_margin_end(8);
        row.set_hexpand(true);

        let command_label = gtk4::Label::new(Some(command));
        command_label.add_css_class("monospace");
        command_label.set_selectable(true);
        command_label.set_xalign(0.0);
        command_label.set_hexpand(true);
        command_label.set_margin_start(10);
        command_label.set_margin_top(8);
        command_label.set_margin_bottom(8);
        row.append(&command_label);

        let copy_button = gtk4::Button::from_icon_name("edit-copy-symbolic");
        copy_button.add_css_class("flat");
        copy_button.set_tooltip_text(Some(tr("Copy command").as_str()));
        copy_button.set_margin_end(8);
        copy_button.set_margin_top(4);
        copy_button.set_margin_bottom(4);
        let command_text = command.clone();
        copy_button.connect_clicked(move |_| {
            if let Some(display) = gtk4::gdk::Display::default() {
                display.clipboard().set_text(&command_text);
            }
        });
        row.append(&copy_button);
        container.append(&row);
    }
}

fn guided_step_title(index: usize) -> String {
    match index {
        0 => tr("Install/update Nautilus extension"),
        1 => tr("Install/update FUSE helper"),
        2 => tr("Restart Files"),
        3 => tr("Re-check integration status"),
        _ => tr("Run command"),
    }
}

fn install_css() {
    let provider = gtk4::CssProvider::new();
    provider.load_from_data(
        ".app-shell { background: alpha(@window_fg_color, 0.015); }\n\
         .app-header { background: alpha(#fc3f1d, 0.10); border-bottom: 1px solid alpha(#fc3f1d, 0.24); }\n\
         .app-title { font-weight: 800; letter-spacing: 0.02em; color: #cf3616; }\n\
         .navigation-sidebar { padding: 10px; border-radius: 20px; background: alpha(#fc3f1d, 0.05); border: 1px solid alpha(#fc3f1d, 0.18); box-shadow: inset 0 1px 0 alpha(#ffffff, 0.03); }\n\
         .navigation-sidebar row { border-radius: 12px; margin: 2px 0; padding: 5px 8px; transition: 120ms ease-out; }\n\
         .navigation-sidebar row:hover { background: alpha(#fc3f1d, 0.07); }\n\
         .navigation-sidebar row:selected { background: #cf3616; box-shadow: inset 0 0 0 1px alpha(#ffb25c, 0.42); }\n\
         .navigation-sidebar row:selected label { color: white; font-weight: 700; }\n\
         .content-stack { min-width: 740px; }\n\
         .overview-strip { padding-top: 8px; padding-bottom: 4px; }\n\
         .overview-tile { padding: 10px 13px; border-radius: 15px; background: alpha(@window_fg_color, 0.035); border: 1px solid alpha(#fc3f1d, 0.14); box-shadow: inset 0 1px 0 alpha(#ffffff, 0.03); }\n\
         .overview-title { opacity: 0.90; font-weight: 700; }\n\
         .page-root { padding: 18px; }\n\
         .page-hero { margin-bottom: 2px; padding: 6px 0 2px 0; }\n\
         .page-heading { margin-bottom: 4px; }\n\
         .section-card { padding: 20px; border-radius: 18px; background: alpha(@window_fg_color, 0.03); border: 1px solid alpha(@window_fg_color, 0.08); box-shadow: inset 0 1px 0 alpha(#ffffff, 0.03); }\n\
         .brand-card { background: alpha(#fc3f1d, 0.07); border: 1px solid alpha(#fc3f1d, 0.24); box-shadow: inset 4px 0 0 #cf3616, inset 0 1px 0 alpha(#ffffff, 0.03); }\n\
         .section-card .title-3 { color: alpha(@window_fg_color, 0.98); }\n\
         .section-muted { background: alpha(#ffb25c, 0.06); border: 1px solid alpha(#ff8f3f, 0.16); }\n\
         .section-muted .title-3 { color: alpha(@window_fg_color, 0.96); }\n\
         .note-card { border-left: 3px solid alpha(#ff8f3f, 0.60); }\n\
         .body-copy { color: alpha(@window_fg_color, 0.94); }\n\
         .note-text { color: alpha(@window_fg_color, 0.72); }\n\
         .action-row > button { min-height: 34px; border-radius: 10px; padding: 0 12px; }\n\
         .action-row > button.suggested-action { background: #cf3616; color: white; box-shadow: inset 0 1px 0 alpha(#ffffff, 0.14); }\n\
         .action-row > button:not(.suggested-action):not(.destructive-action) { border-color: alpha(#fc3f1d, 0.24); }\n\
         .action-row > button.destructive-action { box-shadow: inset 0 1px 0 alpha(#ffffff, 0.08); }\n\
         .pill { border-radius: 999px; padding: 4px 10px; font-weight: 600; }\n\
         .status-ready { background: alpha(@success_bg_color, 0.32); color: @success_fg_color; }\n\
         .status-needs { background: alpha(#ffb25c, 0.20); color: @warning_fg_color; }\n\
         .status-error { background: alpha(@error_bg_color, 0.30); color: @error_fg_color; }\n\
         .status-unknown { background: alpha(@window_fg_color, 0.12); color: alpha(@window_fg_color, 0.88); }\n\
         .accent { color: #cf3616; }\n",
    );
    if let Some(display) = gtk4::gdk::Display::default() {
        gtk4::style_context_add_provider_for_display(
            &display,
            &provider,
            gtk4::STYLE_PROVIDER_PRIORITY_APPLICATION,
        );
    }
}
