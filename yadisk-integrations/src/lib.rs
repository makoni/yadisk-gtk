#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncUiState {
    CloudOnly,
    Cached,
    Syncing,
    Error,
}

pub fn adwaita_symbolic_icon(state: SyncUiState) -> &'static str {
    match state {
        SyncUiState::CloudOnly => "cloud-symbolic",
        SyncUiState::Cached => "emblem-ok-symbolic",
        SyncUiState::Syncing => "view-refresh-symbolic",
        SyncUiState::Error => "dialog-error-symbolic",
    }
}

pub struct NautilusExtensionMvp;

impl NautilusExtensionMvp {
    pub fn emblems_for_state(state: SyncUiState) -> Vec<&'static str> {
        vec![adwaita_symbolic_icon(state)]
    }

    pub fn context_actions() -> Vec<&'static str> {
        vec!["Download", "Pin", "Evict", "Retry"]
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccountHealth {
    Online,
    Offline,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CloudProviderAccount {
    pub id: String,
    pub display_name: String,
    pub sync_root: String,
    pub health: AccountHealth,
}

impl CloudProviderAccount {
    pub fn sidebar_label(&self) -> String {
        format!("{} ({})", self.display_name, self.sync_root)
    }

    pub fn apply_health_from_state(&mut self, state: &str) {
        self.health = match state {
            "online" => AccountHealth::Online,
            "offline" => AccountHealth::Offline,
            _ => AccountHealth::Error,
        };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uses_default_adwaita_icons() {
        assert_eq!(
            adwaita_symbolic_icon(SyncUiState::CloudOnly),
            "cloud-symbolic"
        );
        assert_eq!(
            adwaita_symbolic_icon(SyncUiState::Cached),
            "emblem-ok-symbolic"
        );
        assert_eq!(
            adwaita_symbolic_icon(SyncUiState::Syncing),
            "view-refresh-symbolic"
        );
        assert_eq!(
            adwaita_symbolic_icon(SyncUiState::Error),
            "dialog-error-symbolic"
        );
    }

    #[test]
    fn context_actions_cover_mvp_commands() {
        assert_eq!(
            NautilusExtensionMvp::context_actions(),
            vec!["Download", "Pin", "Evict", "Retry"]
        );
    }

    #[test]
    fn cloud_provider_account_syncs_health() {
        let mut account = CloudProviderAccount {
            id: "acc-1".into(),
            display_name: "Yandex Disk".into(),
            sync_root: "/home/user/YandexDisk".into(),
            health: AccountHealth::Offline,
        };
        account.apply_health_from_state("online");
        assert_eq!(account.health, AccountHealth::Online);
        account.apply_health_from_state("broken");
        assert_eq!(account.health, AccountHealth::Error);
    }
}
