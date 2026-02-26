use crate::ui_model::{UiModel, UiStatus};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    Welcome,
    SyncStatus,
    Integrations,
    Settings,
    Diagnostics,
}

pub const ALL_STATUSES: [UiStatus; 4] = [
    UiStatus::Unknown,
    UiStatus::Ready,
    UiStatus::NeedsSetup,
    UiStatus::Error,
];

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppState {
    pub route: Route,
    pub auth: UiStatus,
    pub daemon: UiStatus,
    pub integrations: UiStatus,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            route: Route::Welcome,
            auth: UiStatus::NeedsSetup,
            daemon: UiStatus::Unknown,
            integrations: UiStatus::Unknown,
        }
    }
}

pub fn sidebar_routes() -> [Route; 5] {
    [
        Route::Welcome,
        Route::SyncStatus,
        Route::Integrations,
        Route::Settings,
        Route::Diagnostics,
    ]
}

pub fn run(model: &UiModel) {
    let state = AppState {
        auth: model.auth_status,
        daemon: model.daemon_status,
        integrations: model.integration_status,
        ..AppState::default()
    };
    let routes = sidebar_routes();
    let statuses = ALL_STATUSES;
    eprintln!(
        "[yadisk-ui] bootstrap ready: route={:?}, auth={:?}, daemon={:?}, integrations={:?}",
        state.route, state.auth, state.daemon, state.integrations
    );
    eprintln!(
        "[yadisk-ui] navigation scaffold initialized: routes={}, statuses={}",
        routes.len(),
        statuses.len()
    );
    if model.control.is_none() {
        eprintln!("[yadisk-ui] control API is unavailable");
    } else {
        eprintln!("[yadisk-ui] auth: {}", model.auth_summary);
        eprintln!("[yadisk-ui] daemon: {}", model.daemon_summary);
    }
    if let Some(service_status) = &model.service {
        eprintln!(
            "[yadisk-ui] service: yadiskd.service state={}",
            service_status.normalized()
        );
    }
    eprintln!(
        "[yadisk-ui] integrations: state={}, details={}",
        model.integrations.summary_state(),
        model.integration_summary
    );
    eprintln!("[yadisk-ui] gtk shell is available via default launch mode");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_state_starts_in_welcome_and_needs_auth() {
        let state = AppState::default();
        assert_eq!(state.route, Route::Welcome);
        assert_eq!(state.auth, UiStatus::NeedsSetup);
        assert_eq!(state.daemon, UiStatus::Unknown);
        assert_eq!(state.integrations, UiStatus::Unknown);
    }

    #[test]
    fn sidebar_contains_expected_routes() {
        let routes = sidebar_routes();
        assert_eq!(
            routes,
            [
                Route::Welcome,
                Route::SyncStatus,
                Route::Integrations,
                Route::Settings,
                Route::Diagnostics,
            ]
        );
    }

    #[test]
    fn states_array_contains_all_statuses() {
        assert_eq!(ALL_STATUSES.len(), 4);
        assert!(ALL_STATUSES.contains(&UiStatus::Ready));
    }
}
