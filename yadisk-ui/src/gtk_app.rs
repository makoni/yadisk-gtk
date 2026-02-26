use std::rc::Rc;

use anyhow::Result;
use gtk4::{gio, glib, prelude::*};
use yadisk_integrations::ids::APP_ID_GTK;

use crate::control_client::ControlClient;
use crate::diagnostics::print_diagnostics_report;
use crate::integration_control::{guided_install_instructions, run_auto_install};
use crate::service_control::{ServiceAction, run_service_action};
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
const ACTION_AUTOSTART_ENABLE: &str = "action-autostart-enable";
const ACTION_AUTOSTART_DISABLE: &str = "action-autostart-disable";
const ACTION_DIAGNOSTICS_DUMP: &str = "action-diagnostics-dump";

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
    settings_label: gtk4::Label,
    diagnostics_label: gtk4::Label,
}

pub fn run(start_tab: Option<String>) -> Result<()> {
    libadwaita::init()?;
    install_css();

    let app = libadwaita::Application::builder()
        .application_id(APP_ID_GTK)
        .flags(gio::ApplicationFlags::NON_UNIQUE)
        .build();

    app.connect_activate(move |app| {
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

        let (
            overview_strip,
            overview_auth_badge,
            overview_daemon_badge,
            overview_integration_badge,
        ) = build_overview_strip();
        let widgets = Rc::new(build_pages(
            &stack,
            overview_auth_badge,
            overview_daemon_badge,
            overview_integration_badge,
        ));
        if let Some(start_tab) = start_tab.as_deref() {
            stack.set_visible_child_name(start_tab);
        }
        refresh_ui(&widgets);
        {
            let widgets_for_tick = Rc::clone(&widgets);
            glib::timeout_add_seconds_local(5, move || {
                refresh_ui(&widgets_for_tick);
                glib::ControlFlow::Continue
            });
        }

        let container = gtk4::Box::new(gtk4::Orientation::Horizontal, 16);
        container.set_margin_start(18);
        container.set_margin_end(18);
        container.set_margin_top(12);
        container.set_margin_bottom(18);
        container.append(&sidebar);
        container.append(&stack);

        let refresh_button = gtk4::Button::from_icon_name("view-refresh-symbolic");
        refresh_button.add_css_class("flat");
        refresh_button.set_tooltip_text(Some("Refresh status"));
        let widgets_for_refresh = Rc::clone(&widgets);
        refresh_button.connect_clicked(move |_| refresh_ui(&widgets_for_refresh));

        let header = libadwaita::HeaderBar::builder()
            .title_widget(&gtk4::Label::new(Some("Yandex Disk")))
            .build();
        header.pack_end(&refresh_button);

        let content = gtk4::Box::new(gtk4::Orientation::Vertical, 0);
        content.append(&header);
        content.append(&overview_strip);
        content.append(&container);

        let window = libadwaita::ApplicationWindow::builder()
            .application(app)
            .title("Yandex Disk")
            .default_width(1060)
            .default_height(760)
            .content(&content)
            .build();

        wire_actions(&stack, Rc::clone(&widgets));
        window.present();
    });

    app.run_with_args::<&str>(&[]);
    Ok(())
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
    let auth_header = section_header("Authorization");
    let auth_badge = status_badge();
    auth_header.append(&auth_badge);
    auth_card.append(&auth_header);
    let account_label = body_label();
    auth_card.append(&account_label);
    let account_actions = action_row();
    let btn_auth_start = gtk4::Button::with_label("Start Auth");
    btn_auth_start.set_widget_name(ACTION_AUTH_START);
    btn_auth_start.add_css_class("suggested-action");
    let btn_auth_cancel = gtk4::Button::with_label("Cancel Auth");
    btn_auth_cancel.set_widget_name(ACTION_AUTH_CANCEL);
    let btn_logout = gtk4::Button::with_label("Logout");
    btn_logout.set_widget_name(ACTION_LOGOUT);
    btn_logout.add_css_class("destructive-action");
    account_actions.append(&btn_auth_start);
    account_actions.append(&btn_auth_cancel);
    account_actions.append(&btn_logout);
    auth_card.append(&account_actions);
    welcome_content.append(&auth_card);
    welcome_content.append(&note_card(
        "Next step",
        "Start Auth, confirm access in the browser, then return here. The page refreshes automatically every few seconds.",
    ));
    stack.add_titled(&welcome, Some("welcome"), "Welcome");

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
    let btn_start = gtk4::Button::with_label("Start");
    btn_start.set_widget_name(ACTION_DAEMON_START);
    btn_start.add_css_class("suggested-action");
    let btn_stop = gtk4::Button::with_label("Stop");
    btn_stop.set_widget_name(ACTION_DAEMON_STOP);
    btn_stop.add_css_class("destructive-action");
    let btn_restart = gtk4::Button::with_label("Restart");
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
    stack.add_titled(&sync, Some("sync"), "Sync Status");

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
    let integration_actions = action_row();
    let btn_check = gtk4::Button::with_label("Re-check");
    btn_check.set_widget_name(ACTION_INTEGRATIONS_CHECK);
    btn_check.add_css_class("suggested-action");
    let btn_guided = gtk4::Button::with_label("Guided Install");
    btn_guided.set_widget_name(ACTION_INTEGRATIONS_GUIDED);
    let btn_auto = gtk4::Button::with_label("Advanced Auto Install");
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
    stack.add_titled(&integrations, Some("integrations"), "Integrations");

    let (settings, settings_content) = page_shell();
    settings_content.append(&page_heading("emblem-system-symbolic", "Settings"));
    settings_content.append(&page_description(
        "Review service folders and startup behavior.",
    ));
    let settings_card = section_card();
    let settings_label = body_label();
    settings_label.add_css_class("monospace");
    settings_card.append(&settings_label);
    let settings_actions = action_row();
    let btn_auto_on = gtk4::Button::with_label("Enable autostart");
    btn_auto_on.set_widget_name(ACTION_AUTOSTART_ENABLE);
    btn_auto_on.add_css_class("suggested-action");
    let btn_auto_off = gtk4::Button::with_label("Disable autostart");
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
    stack.add_titled(&settings, Some("settings"), "Settings");

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
    let btn_dump = gtk4::Button::with_label("Print diagnostics JSON");
    btn_dump.set_widget_name(ACTION_DIAGNOSTICS_DUMP);
    btn_dump.add_css_class("suggested-action");
    diagnostics_actions.append(&btn_dump);
    diagnostics_card.append(&diagnostics_actions);
    diagnostics_content.append(&diagnostics_card);
    diagnostics_content.append(&note_card(
        "Support tip",
        "Use Print diagnostics JSON when reporting issues so service and integration state is captured in one snapshot.",
    ));
    stack.add_titled(&diagnostics, Some("diagnostics"), "Diagnostics");

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
        settings_label,
        diagnostics_label,
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

    let title_label = gtk4::Label::new(Some(title));
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
    let label = gtk4::Label::new(Some(title));
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
    let icon = gtk4::Image::from_icon_name(icon_name);
    icon.set_pixel_size(20);
    icon.add_css_class("accent");
    let title_label = gtk4::Label::new(Some(title));
    title_label.add_css_class("title-2");
    title_label.add_css_class("page-heading");
    title_label.set_halign(gtk4::Align::Start);
    title_label.set_xalign(0.0);
    row.append(&icon);
    row.append(&title_label);
    row
}

fn page_description(text: &str) -> gtk4::Label {
    let label = gtk4::Label::new(Some(text));
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
    card.append(&section_title(title));
    let description = body_label();
    description.add_css_class("note-text");
    description.set_text(text);
    card.append(&description);
    card
}

fn body_label() -> gtk4::Label {
    let label = gtk4::Label::new(None);
    label.set_wrap(true);
    label.set_selectable(true);
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
    let label = gtk4::Label::new(Some("Unknown"));
    label.add_css_class("pill");
    label.add_css_class("status-unknown");
    label.set_halign(gtk4::Align::End);
    label
}

fn wire_actions(stack: &gtk4::Stack, widgets: Rc<Widgets>) {
    if let Some(button) = find_button(stack, ACTION_AUTH_START) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            if let Ok(client) = ControlClient::connect() {
                let _ = client.start_auth();
            }
            refresh_ui(&widgets);
        });
    }
    if let Some(button) = find_button(stack, ACTION_AUTH_CANCEL) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            if let Ok(client) = ControlClient::connect() {
                let _ = client.cancel_auth();
            }
            refresh_ui(&widgets);
        });
    }
    if let Some(button) = find_button(stack, ACTION_LOGOUT) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            if let Ok(client) = ControlClient::connect() {
                let _ = client.logout();
            }
            refresh_ui(&widgets);
        });
    }
    if let Some(button) = find_button(stack, ACTION_DAEMON_START) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            let _ = run_service_action(ServiceAction::Start);
            refresh_ui(&widgets);
        });
    }
    if let Some(button) = find_button(stack, ACTION_DAEMON_STOP) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            let _ = run_service_action(ServiceAction::Stop);
            refresh_ui(&widgets);
        });
    }
    if let Some(button) = find_button(stack, ACTION_DAEMON_RESTART) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            let _ = run_service_action(ServiceAction::Restart);
            refresh_ui(&widgets);
        });
    }
    if let Some(button) = find_button(stack, ACTION_INTEGRATIONS_CHECK) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| refresh_ui(&widgets));
    }
    if let Some(button) = find_button(stack, ACTION_INTEGRATIONS_GUIDED) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            let lines = guided_install_instructions().join("\n");
            widgets.integration_label.set_text(&lines);
        });
    }
    if let Some(button) = find_button(stack, ACTION_INTEGRATIONS_AUTO) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            let _ = run_auto_install();
            refresh_ui(&widgets);
        });
    }
    if let Some(button) = find_button(stack, ACTION_AUTOSTART_ENABLE) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            let _ = run_service_action(ServiceAction::EnableAutostart);
            refresh_ui(&widgets);
        });
    }
    if let Some(button) = find_button(stack, ACTION_AUTOSTART_DISABLE) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            let _ = run_service_action(ServiceAction::DisableAutostart);
            refresh_ui(&widgets);
        });
    }
    if let Some(button) = find_button(stack, ACTION_DIAGNOSTICS_DUMP) {
        let widgets = Rc::clone(&widgets);
        button.connect_clicked(move |_| {
            let model = UiModel::collect();
            let _ = print_diagnostics_report(
                model.control.as_ref(),
                model.service.as_ref(),
                &model.integrations,
                model.settings.clone(),
            );
            refresh_ui(&widgets);
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

fn refresh_ui(widgets: &Widgets) {
    let model = UiModel::collect();
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
    widgets
        .integration_label
        .set_text(&model.integration_summary);
    widgets.settings_label.set_text(&format!(
        "Sync folder: {}\nCache folder: {}\nRemote root: {}\nCloud poll interval (s): {}\nWorker loop (ms): {}\nLocal watcher enabled: {}\nAutostart: {}",
        model.settings.sync_root,
        model.settings.cache_root,
        model.settings.remote_root,
        model.settings.cloud_poll_secs,
        model.settings.worker_loop_ms,
        model.settings.local_watcher_enabled,
        model.settings.autostart
    ));
    widgets.diagnostics_label.set_text(&format!(
        "Daemon status: {:?}\nAuth status: {:?}\nIntegrations status: {:?}",
        model.daemon_status, model.auth_status, model.integration_status
    ));
}

fn update_badge(label: &gtk4::Label, status: crate::ui_model::UiStatus) {
    label.remove_css_class("status-ready");
    label.remove_css_class("status-needs");
    label.remove_css_class("status-error");
    label.remove_css_class("status-unknown");
    match status {
        crate::ui_model::UiStatus::Ready => {
            label.set_text("Ready");
            label.add_css_class("status-ready");
        }
        crate::ui_model::UiStatus::NeedsSetup => {
            label.set_text("Needs setup");
            label.add_css_class("status-needs");
        }
        crate::ui_model::UiStatus::Error => {
            label.set_text("Error");
            label.add_css_class("status-error");
        }
        crate::ui_model::UiStatus::Unknown => {
            label.set_text("Unknown");
            label.add_css_class("status-unknown");
        }
    }
}

fn install_css() {
    let provider = gtk4::CssProvider::new();
    provider.load_from_data(
        ".navigation-sidebar { padding: 8px; border-radius: 16px; background: alpha(@window_bg_color, 0.36); border: 1px solid alpha(@window_fg_color, 0.06); }\n\
         .navigation-sidebar row { border-radius: 10px; margin: 2px 0; padding: 4px 8px; }\n\
         .navigation-sidebar row:selected { background: alpha(@accent_bg_color, 0.16); box-shadow: inset 0 0 0 1px alpha(@accent_bg_color, 0.32); }\n\
         .content-stack { min-width: 740px; }\n\
         .overview-strip { padding-top: 6px; padding-bottom: 4px; }\n\
         .overview-tile { padding: 9px 12px; border-radius: 12px; background: alpha(@window_bg_color, 0.46); border: 1px solid alpha(@window_fg_color, 0.07); }\n\
         .overview-title { opacity: 0.86; font-weight: 600; }\n\
         .page-root { padding: 18px; }\n\
         .page-heading { margin-bottom: 4px; }\n\
         .section-card { padding: 18px; border-radius: 16px; background: alpha(@window_bg_color, 0.58); border: 1px solid alpha(@window_fg_color, 0.09); }\n\
         .section-muted { background: alpha(@window_fg_color, 0.035); border: 1px solid alpha(@window_fg_color, 0.06); }\n\
         .body-copy { color: alpha(@window_fg_color, 0.94); }\n\
         .note-text { color: alpha(@window_fg_color, 0.72); }\n\
         .action-row > button { min-height: 34px; border-radius: 10px; padding: 0 12px; }\n\
         .pill { border-radius: 999px; padding: 4px 10px; font-weight: 600; }\n\
         .status-ready { background: alpha(@success_bg_color, 0.35); color: @success_fg_color; }\n\
         .status-needs { background: alpha(@warning_bg_color, 0.35); color: @warning_fg_color; }\n\
         .status-error { background: alpha(@error_bg_color, 0.35); color: @error_fg_color; }\n\
         .status-unknown { background: alpha(@window_fg_color, 0.12); color: alpha(@window_fg_color, 0.88); }\n",
    );
    if let Some(display) = gtk4::gdk::Display::default() {
        gtk4::style_context_add_provider_for_display(
            &display,
            &provider,
            gtk4::STYLE_PROVIDER_PRIORITY_APPLICATION,
        );
    }
}
