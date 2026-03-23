use std::collections::HashMap;
use std::sync::{OnceLock, RwLock};

use anyhow::Result;

use crate::preferences::{
    LanguagePreference, load_ui_preferences, product_name_for_language, resolve_effective_language,
    save_language_preference,
};

static EFFECTIVE_LANGUAGE: OnceLock<RwLock<String>> = OnceLock::new();
static PO_TRANSLATIONS: OnceLock<HashMap<String, HashMap<String, String>>> = OnceLock::new();

pub fn init() {
    let preference = load_ui_preferences().language_preference;
    set_effective_language(resolve_effective_language(preference));
    let _ = PO_TRANSLATIONS.get_or_init(load_po_translations);
}

pub fn sync_with_saved_language() {
    let preference = load_ui_preferences().language_preference;
    set_effective_language(resolve_effective_language(preference));
}

pub fn apply_language_preference(preference: LanguagePreference) -> Result<bool> {
    let new_language = resolve_effective_language(preference);
    let changed = current_language() != new_language;
    save_language_preference(preference)?;
    set_effective_language(new_language);
    Ok(changed)
}

pub fn current_language() -> String {
    let lock = EFFECTIVE_LANGUAGE.get_or_init(|| RwLock::new("en".to_string()));
    match lock.read() {
        Ok(guard) => guard.clone(),
        Err(poisoned) => poisoned.into_inner().clone(),
    }
}

pub fn tr(message: &str) -> String {
    let catalogs = PO_TRANSLATIONS.get_or_init(load_po_translations);
    let language = current_language();
    catalogs
        .get(&language)
        .and_then(|catalog| catalog.get(message))
        .filter(|translation| !translation.is_empty())
        .cloned()
        .unwrap_or_else(|| message.to_string())
}

pub fn product_name() -> &'static str {
    product_name_for_language(current_language().as_str())
}

fn set_effective_language(language: String) {
    let lock = EFFECTIVE_LANGUAGE.get_or_init(|| RwLock::new("en".to_string()));
    let mut guard = match lock.write() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    *guard = language;
}

fn load_po_translations() -> HashMap<String, HashMap<String, String>> {
    let mut catalogs = HashMap::new();
    catalogs.insert(
        "ru".to_string(),
        parse_po_catalog(include_str!("../../po/ru.po")),
    );
    catalogs
}

fn parse_po_catalog(content: &str) -> HashMap<String, String> {
    #[derive(Clone, Copy)]
    enum ParseMode {
        MsgId,
        MsgStr,
    }

    let mut translations = HashMap::new();
    let mut current_msgid = String::new();
    let mut current_msgstr = String::new();
    let mut mode: Option<ParseMode> = None;

    let flush_entry =
        |translations: &mut HashMap<String, String>, msgid: &mut String, msgstr: &mut String| {
            if !msgid.is_empty() && !msgstr.is_empty() {
                translations.insert(msgid.clone(), msgstr.clone());
            }
            msgid.clear();
            msgstr.clear();
        };

    for raw_line in content.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            flush_entry(&mut translations, &mut current_msgid, &mut current_msgstr);
            mode = None;
            continue;
        }
        if line.starts_with('#') {
            continue;
        }

        if line.starts_with("msgid ") {
            flush_entry(&mut translations, &mut current_msgid, &mut current_msgstr);
            current_msgid = parse_po_quoted(line);
            current_msgstr.clear();
            mode = Some(ParseMode::MsgId);
            continue;
        }

        if line.starts_with("msgstr ") {
            current_msgstr = parse_po_quoted(line);
            mode = Some(ParseMode::MsgStr);
            continue;
        }

        if line.starts_with('"') {
            match mode {
                Some(ParseMode::MsgId) => current_msgid.push_str(parse_po_quoted(line).as_str()),
                Some(ParseMode::MsgStr) => current_msgstr.push_str(parse_po_quoted(line).as_str()),
                None => {}
            }
        }
    }

    flush_entry(&mut translations, &mut current_msgid, &mut current_msgstr);
    translations
}

fn parse_po_quoted(line: &str) -> String {
    let start = line.find('"').unwrap_or(line.len());
    let end = line.rfind('"').unwrap_or(start);
    if start >= end {
        return String::new();
    }
    line[start + 1..end]
        .replace("\\n", "\n")
        .replace("\\\"", "\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_ru_catalog() {
        let catalog = parse_po_catalog("msgid \"Hello\"\nmsgstr \"Привет\"\n");
        assert_eq!(catalog.get("Hello"), Some(&"Привет".to_string()));
    }
}
