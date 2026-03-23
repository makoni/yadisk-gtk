use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum LanguagePreference {
    #[default]
    System,
    En,
    Ru,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct UiPreferences {
    #[serde(default)]
    pub language_preference: LanguagePreference,
}

pub fn load_ui_preferences() -> UiPreferences {
    let Some(path) = preferences_path() else {
        return UiPreferences::default();
    };
    let Ok(data) = fs::read_to_string(path) else {
        return UiPreferences::default();
    };
    serde_json::from_str(&data).unwrap_or_default()
}

pub fn save_language_preference(language_preference: LanguagePreference) -> Result<()> {
    let mut preferences = load_ui_preferences();
    preferences.language_preference = language_preference;
    save_ui_preferences(&preferences)
}

pub fn save_ui_preferences(preferences: &UiPreferences) -> Result<()> {
    let path = preferences_path().context("could not determine preferences path")?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create preferences dir {}", parent.display()))?;
    }
    let data = serde_json::to_string_pretty(preferences)?;
    fs::write(&path, data)
        .with_context(|| format!("failed to write preferences to {}", path.display()))?;
    Ok(())
}

pub fn preferences_path() -> Option<PathBuf> {
    dirs::config_dir().map(|dir| dir.join("yadisk-gtk").join("preferences.json"))
}

pub fn resolve_effective_language(preference: LanguagePreference) -> String {
    match preference {
        LanguagePreference::System => resolve_system_language().unwrap_or_else(|| "en".to_string()),
        LanguagePreference::En => "en".to_string(),
        LanguagePreference::Ru => "ru".to_string(),
    }
}

pub fn resolve_system_language() -> Option<String> {
    [
        std::env::var("YADISK_UI_LANGUAGE").ok(),
        std::env::var("LC_ALL").ok(),
        std::env::var("LC_MESSAGES").ok(),
        std::env::var("LANGUAGE").ok(),
        std::env::var("LANG").ok(),
    ]
    .into_iter()
    .flatten()
    .find_map(|value| parse_locale_string(&value))
}

pub fn parse_locale_string(locale: &str) -> Option<String> {
    let normalized = locale
        .trim()
        .replace('-', "_")
        .split('.')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    if normalized.starts_with("ru") {
        Some("ru".to_string())
    } else if normalized.starts_with("en") {
        Some("en".to_string())
    } else {
        None
    }
}

pub fn product_name_for_language(language: &str) -> &'static str {
    if language == "ru" {
        "Яндекс Диск"
    } else {
        "Yandex Disk"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_supported_locales() {
        assert_eq!(parse_locale_string("ru_RU.UTF-8"), Some("ru".to_string()));
        assert_eq!(parse_locale_string("en_US.UTF-8"), Some("en".to_string()));
        assert_eq!(parse_locale_string("de_DE.UTF-8"), None);
    }

    #[test]
    fn localizes_product_name() {
        assert_eq!(product_name_for_language("ru"), "Яндекс Диск");
        assert_eq!(product_name_for_language("en"), "Yandex Disk");
        assert_eq!(product_name_for_language("de"), "Yandex Disk");
    }
}
