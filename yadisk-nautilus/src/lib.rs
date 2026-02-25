use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use thiserror::Error;
use yadisk_integrations::ids::{DBUS_INTERFACE_SYNC, DBUS_NAME_SYNC, DBUS_OBJECT_PATH_SYNC};
use zbus::Message;
use zbus::blocking::{Connection, Proxy, proxy::SignalIterator};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncUiState {
    CloudOnly,
    Cached,
    Partial,
    Syncing,
    Error,
}

impl SyncUiState {
    pub fn from_dbus(value: &str) -> Self {
        match value {
            "cached" => Self::Cached,
            "partial" => Self::Partial,
            "syncing" => Self::Syncing,
            "error" => Self::Error,
            _ => Self::CloudOnly,
        }
    }

    pub fn as_dbus(self) -> &'static str {
        match self {
            Self::CloudOnly => "cloud_only",
            Self::Cached => "cached",
            Self::Partial => "partial",
            Self::Syncing => "syncing",
            Self::Error => "error",
        }
    }

    pub fn badge_label(self) -> &'static str {
        match self {
            Self::CloudOnly => "Only in cloud",
            Self::Cached => "Available offline",
            Self::Partial => "Partially available offline",
            Self::Syncing => "Syncing",
            Self::Error => "Sync error",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NautilusAction {
    SaveOffline,
    RemoveOfflineCopy,
    DownloadNow,
    RetrySync,
}

impl NautilusAction {
    pub fn id(self) -> &'static str {
        match self {
            Self::SaveOffline => "save_offline",
            Self::RemoveOfflineCopy => "remove_offline_copy",
            Self::DownloadNow => "download_now",
            Self::RetrySync => "retry_sync",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::SaveOffline => "Save Offline",
            Self::RemoveOfflineCopy => "Remove Offline Copy",
            Self::DownloadNow => "Download",
            Self::RetrySync => "Retry Sync",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MenuItemSpec {
    pub id: &'static str,
    pub label: &'static str,
    pub action: NautilusAction,
    pub is_primary: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileUiInfo {
    pub state: SyncUiState,
    pub emblem: &'static str,
    pub badge_label: &'static str,
    pub menu: Vec<MenuItemSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncSignalEvent {
    StateChanged {
        path: String,
        state: SyncUiState,
    },
    ConflictAdded {
        id: u64,
        path: String,
        renamed_local: String,
    },
}

pub fn emblem_for_state(state: SyncUiState) -> &'static str {
    match state {
        SyncUiState::CloudOnly => "cloud-outline-thin-symbolic",
        SyncUiState::Cached => "check-round-outline-symbolic",
        SyncUiState::Partial => "cloud-outline-thin-symbolic",
        SyncUiState::Syncing => "update-symbolic",
        SyncUiState::Error => "dialog-error-symbolic",
    }
}

pub fn visible_actions_for_state(state: SyncUiState) -> Vec<NautilusAction> {
    match state {
        SyncUiState::CloudOnly => vec![NautilusAction::DownloadNow],
        SyncUiState::Partial => {
            vec![
                NautilusAction::DownloadNow,
                NautilusAction::RemoveOfflineCopy,
            ]
        }
        SyncUiState::Cached => vec![NautilusAction::RemoveOfflineCopy, NautilusAction::RetrySync],
        SyncUiState::Syncing => vec![NautilusAction::RetrySync],
        SyncUiState::Error => vec![NautilusAction::RetrySync, NautilusAction::DownloadNow],
    }
}

pub fn menu_for_state(state: SyncUiState) -> Vec<MenuItemSpec> {
    visible_actions_for_state(state)
        .into_iter()
        .enumerate()
        .map(|(idx, action)| MenuItemSpec {
            id: action.id(),
            label: action.label(),
            action,
            is_primary: idx == 0,
        })
        .collect()
}

#[derive(Debug, Error)]
pub enum ExtensionError {
    #[error("dbus error: {0}")]
    Dbus(#[from] zbus::Error),
    #[error("fdo error: {0}")]
    Fdo(#[from] zbus::fdo::Error),
    #[error("path is outside sync root")]
    OutsideSyncRoot,
    #[error("unsupported signal payload: {0}")]
    UnsupportedSignal(String),
    #[error("empty remote candidate list")]
    EmptyCandidates,
}

pub struct SyncDbusClient {
    connection: Connection,
}

impl SyncDbusClient {
    pub fn connect_session() -> Result<Self, ExtensionError> {
        Ok(Self {
            connection: Connection::session()?,
        })
    }

    fn proxy(&self) -> Result<Proxy<'_>, ExtensionError> {
        Ok(Proxy::new(
            &self.connection,
            DBUS_NAME_SYNC,
            DBUS_OBJECT_PATH_SYNC,
            DBUS_INTERFACE_SYNC,
        )?)
    }

    pub fn get_state(&self, remote_path: &str) -> Result<SyncUiState, ExtensionError> {
        let proxy = self.proxy()?;
        let state: String = proxy.call("GetState", &(remote_path))?;
        Ok(SyncUiState::from_dbus(&state))
    }

    pub fn save_offline(&self, remote_path: &str) -> Result<(), ExtensionError> {
        let proxy = self.proxy()?;
        proxy.call_method("Pin", &(remote_path, true))?;
        proxy.call_method("Download", &(remote_path))?;
        Ok(())
    }

    pub fn download(&self, remote_path: &str) -> Result<(), ExtensionError> {
        let proxy = self.proxy()?;
        proxy.call_method("Download", &(remote_path))?;
        Ok(())
    }

    pub fn pin(&self, remote_path: &str) -> Result<(), ExtensionError> {
        let proxy = self.proxy()?;
        proxy.call_method("Pin", &(remote_path, true))?;
        Ok(())
    }

    pub fn remove_offline_copy(&self, remote_path: &str) -> Result<(), ExtensionError> {
        let proxy = self.proxy()?;
        proxy.call_method("Evict", &(remote_path))?;
        Ok(())
    }

    pub fn retry(&self, remote_path: &str) -> Result<(), ExtensionError> {
        let proxy = self.proxy()?;
        proxy.call_method("Retry", &(remote_path))?;
        Ok(())
    }

    pub fn perform_action(
        &self,
        remote_path: &str,
        action: NautilusAction,
    ) -> Result<(), ExtensionError> {
        match action {
            NautilusAction::SaveOffline => self.save_offline(remote_path),
            NautilusAction::RemoveOfflineCopy => self.remove_offline_copy(remote_path),
            NautilusAction::DownloadNow => self.download(remote_path),
            NautilusAction::RetrySync => self.retry(remote_path),
        }
    }

    pub fn perform_action_with_fallback(
        &self,
        remote_candidates: &[String],
        action: NautilusAction,
    ) -> Result<(), ExtensionError> {
        if remote_candidates.is_empty() {
            return Err(ExtensionError::EmptyCandidates);
        }
        let mut last_err: Option<ExtensionError> = None;
        for candidate in remote_candidates {
            match self.perform_action(candidate, action) {
                Ok(_) => return Ok(()),
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or(ExtensionError::EmptyCandidates))
    }

    pub fn get_state_with_fallback(
        &self,
        remote_candidates: &[String],
    ) -> Result<SyncUiState, ExtensionError> {
        if remote_candidates.is_empty() {
            return Err(ExtensionError::EmptyCandidates);
        }
        let mut last_err: Option<ExtensionError> = None;
        for candidate in remote_candidates {
            match self.get_state(candidate) {
                Ok(state) => return Ok(state),
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or(ExtensionError::EmptyCandidates))
    }

    pub fn subscribe_signals(&self) -> Result<SignalListener, ExtensionError> {
        let proxy = self.proxy()?;
        let iter = proxy.receive_all_signals()?;
        Ok(SignalListener { iter })
    }
}

pub fn map_local_to_remote_candidates(
    local_path: &Path,
    sync_root: &Path,
) -> Result<[String; 2], ExtensionError> {
    let relative = local_path
        .strip_prefix(sync_root)
        .map_err(|_| ExtensionError::OutsideSyncRoot)?;
    let suffix = relative.to_string_lossy().replace('\\', "/");
    let normalized = suffix.trim_start_matches('/');
    Ok([format!("disk:/{}", normalized), format!("/{}", normalized)])
}

pub fn map_remote_to_local_path(remote_path: &str, sync_root: &Path) -> PathBuf {
    let normalized = if let Some(rest) = remote_path.strip_prefix("disk:/") {
        format!("/{}", rest.trim_start_matches('/'))
    } else {
        remote_path.to_string()
    };
    let mut local = PathBuf::from(sync_root);
    for part in normalized.split('/').filter(|part| !part.is_empty()) {
        local.push(part);
    }
    local
}

pub struct NautilusInfoProvider {
    sync_root: PathBuf,
    client: Arc<SyncDbusClient>,
    cache: Mutex<HashMap<PathBuf, SyncUiState>>,
}

impl NautilusInfoProvider {
    pub fn new(sync_root: PathBuf, client: Arc<SyncDbusClient>) -> Self {
        Self {
            sync_root,
            client,
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub fn info_for_path(&self, local_path: &Path) -> Result<FileUiInfo, ExtensionError> {
        let candidates = map_local_to_remote_candidates(local_path, &self.sync_root)?;
        let state = self.client.get_state_with_fallback(&candidates)?;
        self.cache
            .lock()
            .expect("cache lock poisoned")
            .insert(local_path.to_path_buf(), state);
        Ok(FileUiInfo {
            state,
            emblem: emblem_for_state(state),
            badge_label: state.badge_label(),
            menu: menu_for_state(state),
        })
    }

    pub fn apply_signal(&self, event: &SyncSignalEvent) {
        if let SyncSignalEvent::StateChanged { path, state } = event {
            let local = map_remote_to_local_path(path, &self.sync_root);
            self.cache
                .lock()
                .expect("cache lock poisoned")
                .insert(local, *state);
        }
    }
}

pub struct NautilusMenuProvider {
    sync_root: PathBuf,
    client: Arc<SyncDbusClient>,
}

impl NautilusMenuProvider {
    pub fn new(sync_root: PathBuf, client: Arc<SyncDbusClient>) -> Self {
        Self { sync_root, client }
    }

    pub fn menu_for_path(&self, local_path: &Path) -> Result<Vec<MenuItemSpec>, ExtensionError> {
        let candidates = map_local_to_remote_candidates(local_path, &self.sync_root)?;
        let state = self.client.get_state_with_fallback(&candidates)?;
        Ok(menu_for_state(state))
    }

    pub fn activate_action(
        &self,
        local_path: &Path,
        action: NautilusAction,
    ) -> Result<(), ExtensionError> {
        let candidates = map_local_to_remote_candidates(local_path, &self.sync_root)?;
        self.client
            .perform_action_with_fallback(&candidates, action)
    }
}

pub struct SignalListener {
    iter: SignalIterator<'static>,
}

impl SignalListener {
    pub fn next_event(&mut self) -> Result<Option<SyncSignalEvent>, ExtensionError> {
        let Some(message) = self.iter.next() else {
            return Ok(None);
        };
        parse_signal_event(&message).map(Some)
    }
}

fn parse_signal_event(message: &Message) -> Result<SyncSignalEvent, ExtensionError> {
    let member = message
        .header()
        .member()
        .map(|member| member.as_str().to_string())
        .unwrap_or_default();

    match member.as_str() {
        "StateChanged" => {
            let (path, state): (String, String) = message.body().deserialize()?;
            Ok(SyncSignalEvent::StateChanged {
                path,
                state: SyncUiState::from_dbus(&state),
            })
        }
        "ConflictAdded" => {
            let (id, path, renamed_local): (u64, String, String) = message.body().deserialize()?;
            Ok(SyncSignalEvent::ConflictAdded {
                id,
                path,
                renamed_local,
            })
        }
        other => Err(ExtensionError::UnsupportedSignal(other.to_string())),
    }
}

#[cfg(feature = "nautilus-plugin")]
mod nautilus_plugin {
    #![allow(unsafe_op_in_unsafe_fn)]

    use super::*;
    use std::ffi::{CStr, CString};
    use std::os::raw::{c_char, c_int};
    use std::ptr;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Mutex, Once, OnceLock, RwLock};
    use std::thread;

    use glib_sys::{GList, GType, g_free, g_list_append, gpointer};
    use gobject_sys::{
        GClosure, GInterfaceInfo, GObject, GObjectClass, GTypeInfo, GTypeInterface, GTypeModule,
        g_object_get_type, g_object_unref, g_signal_connect_data, g_type_module_add_interface,
        g_type_module_register_type,
    };
    use url::Url;

    #[repr(C)]
    struct NautilusFileInfo {
        _private: [u8; 0],
    }

    #[repr(C)]
    struct NautilusMenuItem {
        _private: [u8; 0],
    }

    #[repr(C)]
    struct NautilusMenuProvider {
        _private: [u8; 0],
    }

    #[repr(C)]
    struct NautilusInfoProvider {
        _private: [u8; 0],
    }

    #[repr(C)]
    struct NautilusOperationHandle {
        _private: [u8; 0],
    }

    #[repr(C)]
    #[derive(Clone, Copy)]
    #[allow(dead_code)]
    enum NautilusOperationResult {
        Complete = 0,
        Failed = 1,
        InProgress = 2,
    }

    #[repr(C)]
    struct NautilusInfoProviderInterface {
        g_iface: GTypeInterface,
        update_file_info: Option<
            unsafe extern "C" fn(
                provider: *mut NautilusInfoProvider,
                file: *mut NautilusFileInfo,
                update_complete: *mut GClosure,
                handle: *mut *mut NautilusOperationHandle,
            ) -> NautilusOperationResult,
        >,
        cancel_update: Option<
            unsafe extern "C" fn(
                provider: *mut NautilusInfoProvider,
                handle: *mut NautilusOperationHandle,
            ),
        >,
    }

    #[repr(C)]
    struct NautilusMenuProviderInterface {
        g_iface: GTypeInterface,
        get_file_items: Option<
            unsafe extern "C" fn(
                provider: *mut NautilusMenuProvider,
                files: *mut GList,
            ) -> *mut GList,
        >,
        get_background_items: Option<
            unsafe extern "C" fn(
                provider: *mut NautilusMenuProvider,
                current_folder: *mut NautilusFileInfo,
            ) -> *mut GList,
        >,
    }

    #[link(name = "nautilus-extension")]
    unsafe extern "C" {
        fn nautilus_info_provider_get_type() -> GType;
        fn nautilus_menu_provider_get_type() -> GType;

        fn nautilus_file_info_get_uri(file_info: *mut NautilusFileInfo) -> *mut c_char;
        fn nautilus_file_info_add_emblem(
            file_info: *mut NautilusFileInfo,
            emblem_name: *const c_char,
        );
        fn nautilus_file_info_add_string_attribute(
            file_info: *mut NautilusFileInfo,
            attribute_name: *const c_char,
            value: *const c_char,
        );
        fn nautilus_file_info_invalidate_extension_info(file_info: *mut NautilusFileInfo);
        fn nautilus_file_info_lookup_for_uri(uri: *const c_char) -> *mut NautilusFileInfo;

        fn nautilus_menu_item_new(
            name: *const c_char,
            label: *const c_char,
            tip: *const c_char,
            icon: *const c_char,
        ) -> *mut NautilusMenuItem;
    }

    #[derive(Clone)]
    struct ActionContext {
        action: NautilusAction,
        local_paths: Vec<PathBuf>,
    }

    static CLIENT: OnceLock<Option<Arc<SyncDbusClient>>> = OnceLock::new();
    static SYNC_ROOT: OnceLock<PathBuf> = OnceLock::new();
    static STATE_CACHE: OnceLock<RwLock<HashMap<String, SyncUiState>>> = OnceLock::new();
    static ACTION_CONTEXTS: OnceLock<Mutex<HashMap<usize, ActionContext>>> = OnceLock::new();
    static START_SIGNAL_THREAD: Once = Once::new();
    static SIGNAL_THREAD_STARTED: AtomicBool = AtomicBool::new(false);
    static REGISTERED_TYPE: AtomicUsize = AtomicUsize::new(0);
    static REGISTERED_TYPES: OnceLock<[GType; 1]> = OnceLock::new();

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn nautilus_module_initialize(module: *mut GTypeModule) {
        if module.is_null() {
            return;
        }
        let registered = ensure_registered_type(module);
        let _ = REGISTERED_TYPES.set([registered]);
        let _ = dbus_client();
        let _ = state_cache();
        let _ = action_contexts();
        start_signal_thread_once();
    }

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn nautilus_module_shutdown() {}

    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn nautilus_module_list_types(
        types: *mut *const GType,
        num_types: *mut c_int,
    ) {
        if !types.is_null() {
            if let Some(registered) = REGISTERED_TYPES.get() {
                *types = registered.as_ptr();
            } else {
                *types = ptr::null();
            }
        }
        if !num_types.is_null() {
            *num_types = if REGISTERED_TYPES.get().is_some() {
                1
            } else {
                0
            };
        }
    }

    fn ensure_registered_type(module: *mut GTypeModule) -> GType {
        let existing = REGISTERED_TYPE.load(Ordering::SeqCst);
        if existing != 0 {
            return existing as GType;
        }
        let registered = unsafe { register_extension_type(module) };
        REGISTERED_TYPE.store(registered as usize, Ordering::SeqCst);
        registered
    }

    unsafe fn register_extension_type(module: *mut GTypeModule) -> GType {
        let type_name = CString::new("YadiskRustExtension").expect("valid type name");

        let mut type_info: GTypeInfo = std::mem::zeroed();
        type_info.class_size = std::mem::size_of::<GObjectClass>() as u16;
        type_info.instance_size = std::mem::size_of::<GObject>() as u16;

        let extension_type = g_type_module_register_type(
            module,
            g_object_get_type(),
            type_name.as_ptr(),
            &type_info,
            0,
        );

        let mut info_iface: GInterfaceInfo = std::mem::zeroed();
        info_iface.interface_init = Some(info_provider_iface_init);

        let mut menu_iface: GInterfaceInfo = std::mem::zeroed();
        menu_iface.interface_init = Some(menu_provider_iface_init);

        g_type_module_add_interface(
            module,
            extension_type,
            nautilus_info_provider_get_type(),
            &info_iface,
        );
        g_type_module_add_interface(
            module,
            extension_type,
            nautilus_menu_provider_get_type(),
            &menu_iface,
        );

        extension_type
    }

    unsafe extern "C" fn info_provider_iface_init(iface: gpointer, _iface_data: gpointer) {
        let iface = iface as *mut NautilusInfoProviderInterface;
        if iface.is_null() {
            return;
        }
        (*iface).update_file_info = Some(info_provider_update_file_info);
        (*iface).cancel_update = Some(info_provider_cancel_update);
    }

    unsafe extern "C" fn menu_provider_iface_init(iface: gpointer, _iface_data: gpointer) {
        let iface = iface as *mut NautilusMenuProviderInterface;
        if iface.is_null() {
            return;
        }
        (*iface).get_file_items = Some(menu_provider_get_file_items);
        (*iface).get_background_items = None;
    }

    unsafe extern "C" fn info_provider_update_file_info(
        _provider: *mut NautilusInfoProvider,
        file: *mut NautilusFileInfo,
        _update_complete: *mut GClosure,
        _handle: *mut *mut NautilusOperationHandle,
    ) -> NautilusOperationResult {
        let Some(local_path) = file_info_to_local_path(file) else {
            return NautilusOperationResult::Complete;
        };

        let state = state_for_local_path(&local_path).unwrap_or(SyncUiState::CloudOnly);

        let emblem = CString::new(emblem_for_state(state)).expect("valid emblem");
        let attr_state_name = CString::new("yadisk::state").expect("valid attr name");
        let attr_status_name = CString::new("yadisk::status").expect("valid attr name");
        let attr_state_value = CString::new(state.as_dbus()).expect("valid attr value");
        let attr_status_value = CString::new(state.badge_label()).expect("valid attr value");

        nautilus_file_info_add_emblem(file, emblem.as_ptr());
        nautilus_file_info_add_string_attribute(
            file,
            attr_state_name.as_ptr(),
            attr_state_value.as_ptr(),
        );
        nautilus_file_info_add_string_attribute(
            file,
            attr_status_name.as_ptr(),
            attr_status_value.as_ptr(),
        );

        NautilusOperationResult::Complete
    }

    unsafe extern "C" fn info_provider_cancel_update(
        _provider: *mut NautilusInfoProvider,
        _handle: *mut NautilusOperationHandle,
    ) {
    }

    unsafe extern "C" fn menu_provider_get_file_items(
        _provider: *mut NautilusMenuProvider,
        files: *mut GList,
    ) -> *mut GList {
        let local_paths = file_infos_to_local_paths(files);
        if local_paths.is_empty() {
            return ptr::null_mut();
        }

        let state = state_for_local_path(&local_paths[0]).unwrap_or(SyncUiState::CloudOnly);
        let mut items: *mut GList = ptr::null_mut();
        for spec in menu_for_state(state) {
            let item = create_menu_item(
                &format!("YadiskRust::{}", spec.id),
                spec.label,
                "Yandex Disk action",
            );
            if item.is_null() {
                continue;
            }
            attach_action_context(item, spec.action, &local_paths);
            items = g_list_append(items, item as gpointer);
        }
        items
    }

    fn create_menu_item(name: &str, label: &str, tip: &str) -> *mut NautilusMenuItem {
        let Ok(name) = CString::new(name) else {
            return ptr::null_mut();
        };
        let Ok(label) = CString::new(label) else {
            return ptr::null_mut();
        };
        let Ok(tip) = CString::new(tip) else {
            return ptr::null_mut();
        };
        unsafe { nautilus_menu_item_new(name.as_ptr(), label.as_ptr(), tip.as_ptr(), ptr::null()) }
    }

    fn attach_action_context(
        item: *mut NautilusMenuItem,
        action: NautilusAction,
        local_paths: &[PathBuf],
    ) {
        if item.is_null() {
            return;
        }

        if let Ok(mut contexts) = action_contexts().lock() {
            if contexts.len() > 8192 {
                contexts.clear();
            }
            contexts.insert(
                item as usize,
                ActionContext {
                    action,
                    local_paths: local_paths.to_vec(),
                },
            );
        }

        let Ok(signal_name) = CString::new("activate") else {
            return;
        };

        #[allow(clippy::missing_transmute_annotations)]
        let callback = Some(unsafe {
            std::mem::transmute::<
                unsafe extern "C" fn(*mut NautilusMenuItem, gpointer),
                unsafe extern "C" fn(),
            >(menu_item_activate_cb)
        });

        unsafe {
            g_signal_connect_data(
                item as *mut GObject,
                signal_name.as_ptr(),
                callback,
                ptr::null_mut(),
                None,
                0,
            );
        }
    }

    unsafe extern "C" fn menu_item_activate_cb(item: *mut NautilusMenuItem, _user_data: gpointer) {
        let context = {
            let Ok(contexts) = action_contexts().lock() else {
                eprintln!("[yadisk-nautilus] action context lock failed");
                return;
            };
            contexts.get(&(item as usize)).cloned()
        };
        let Some(context) = context else {
            eprintln!("[yadisk-nautilus] action context not found");
            return;
        };

        let Some(client) = dbus_client().cloned() else {
            eprintln!("[yadisk-nautilus] dbus client unavailable");
            return;
        };

        for local_path in context.local_paths {
            let Ok(candidates) = map_local_to_remote_candidates(&local_path, sync_root()) else {
                continue;
            };
            eprintln!(
                "[yadisk-nautilus] action {:?} request for {}",
                context.action,
                local_path.display()
            );
            match client.perform_action_with_fallback(&candidates, context.action) {
                Ok(_) => {
                    eprintln!(
                        "[yadisk-nautilus] action {:?} queued for {}",
                        context.action,
                        local_path.display()
                    );
                    invalidate_file_info_for_local_path(&local_path);
                    invalidate_parent_info_for_local_path(&local_path);
                }
                Err(err) => eprintln!(
                    "[yadisk-nautilus] action {:?} failed for {}: {}",
                    context.action,
                    local_path.display(),
                    err
                ),
            }
        }
    }

    fn file_infos_to_local_paths(mut files: *mut GList) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        unsafe {
            while !files.is_null() {
                let file_info = (*files).data as *mut NautilusFileInfo;
                if let Some(path) = file_info_to_local_path(file_info) {
                    paths.push(path);
                }
                files = (*files).next;
            }
        }
        paths
    }

    fn file_info_to_local_path(file_info: *mut NautilusFileInfo) -> Option<PathBuf> {
        if file_info.is_null() {
            return None;
        }

        unsafe {
            let uri_ptr = nautilus_file_info_get_uri(file_info);
            if uri_ptr.is_null() {
                return None;
            }

            let uri = CStr::from_ptr(uri_ptr).to_string_lossy().into_owned();
            g_free(uri_ptr as gpointer);

            let parsed = Url::parse(&uri).ok()?;
            if parsed.scheme() != "file" {
                return None;
            }
            let path = parsed.to_file_path().ok()?;
            if !path.starts_with(sync_root()) {
                return None;
            }
            Some(path)
        }
    }

    fn sync_root() -> &'static PathBuf {
        SYNC_ROOT.get_or_init(|| {
            std::env::var("YADISK_SYNC_DIR")
                .map(PathBuf::from)
                .unwrap_or_else(|_| {
                    dirs::home_dir()
                        .unwrap_or_else(|| PathBuf::from("/"))
                        .join("Yandex Disk")
                })
        })
    }

    fn dbus_client() -> Option<&'static Arc<SyncDbusClient>> {
        CLIENT
            .get_or_init(|| SyncDbusClient::connect_session().ok().map(Arc::new))
            .as_ref()
    }

    fn state_cache() -> &'static RwLock<HashMap<String, SyncUiState>> {
        STATE_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
    }

    fn action_contexts() -> &'static Mutex<HashMap<usize, ActionContext>> {
        ACTION_CONTEXTS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    fn cache_state(remote_path: &str, state: SyncUiState) {
        let Ok(mut cache) = state_cache().write() else {
            return;
        };

        cache.insert(remote_path.to_string(), state);
        if let Some(rest) = remote_path.strip_prefix("disk:/") {
            cache.insert(format!("/{}", rest.trim_start_matches('/')), state);
        } else if let Some(rest) = remote_path.strip_prefix('/') {
            cache.insert(format!("disk:/{}", rest), state);
        }
    }

    fn state_for_local_path(local_path: &Path) -> Result<SyncUiState, ExtensionError> {
        let candidates = map_local_to_remote_candidates(local_path, sync_root())?;
        let client = dbus_client().ok_or(ExtensionError::Dbus(zbus::Error::Failure(
            "D-Bus unavailable".into(),
        )))?;

        if let Ok(state) = client.get_state_with_fallback(&candidates) {
            cache_state(&candidates[0], state);
            cache_state(&candidates[1], state);
            return Ok(state);
        }

        if let Ok(cache) = state_cache().read() {
            for candidate in &candidates {
                if let Some(state) = cache.get(candidate) {
                    return Ok(*state);
                }
            }
        }
        client.get_state_with_fallback(&candidates)
    }

    fn invalidate_file_info_for_local_path(local_path: &Path) {
        let Ok(uri) = Url::from_file_path(local_path) else {
            return;
        };
        let Ok(uri_c) = CString::new(uri.as_str()) else {
            return;
        };

        unsafe {
            let file_info = nautilus_file_info_lookup_for_uri(uri_c.as_ptr());
            if file_info.is_null() {
                return;
            }
            nautilus_file_info_invalidate_extension_info(file_info);
            g_object_unref(file_info as *mut GObject);
        }
    }

    fn invalidate_parent_info_for_local_path(local_path: &Path) {
        let Some(parent) = local_path.parent() else {
            return;
        };
        invalidate_file_info_for_local_path(parent);
    }

    fn start_signal_thread_once() {
        START_SIGNAL_THREAD.call_once(|| {
            let Some(client) = dbus_client().cloned() else {
                return;
            };
            SIGNAL_THREAD_STARTED.store(true, Ordering::SeqCst);

            thread::spawn(move || {
                let Ok(mut listener) = client.subscribe_signals() else {
                    return;
                };

                while let Ok(Some(event)) = listener.next_event() {
                    match event {
                        SyncSignalEvent::StateChanged { path, state } => {
                            cache_state(&path, state);
                            let local_path = map_remote_to_local_path(&path, sync_root());
                            eprintln!(
                                "[yadisk-nautilus] state changed: path={} state={}",
                                path,
                                state.as_dbus()
                            );
                            invalidate_file_info_for_local_path(&local_path);
                            invalidate_parent_info_for_local_path(&local_path);
                        }
                        SyncSignalEvent::ConflictAdded { .. } => {}
                    }
                }
            });
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_state_to_emblem_and_actions() {
        assert_eq!(
            emblem_for_state(SyncUiState::CloudOnly),
            "cloud-outline-thin-symbolic"
        );
        assert_eq!(
            visible_actions_for_state(SyncUiState::CloudOnly),
            vec![NautilusAction::DownloadNow]
        );
        assert_eq!(
            visible_actions_for_state(SyncUiState::Partial),
            vec![
                NautilusAction::DownloadNow,
                NautilusAction::RemoveOfflineCopy
            ]
        );
        assert_eq!(
            visible_actions_for_state(SyncUiState::Cached),
            vec![NautilusAction::RemoveOfflineCopy, NautilusAction::RetrySync]
        );
        assert_eq!(
            menu_for_state(SyncUiState::CloudOnly)
                .first()
                .map(|item| item.label),
            Some("Download")
        );
        assert_eq!(
            emblem_for_state(SyncUiState::Partial),
            "cloud-outline-thin-symbolic"
        );
    }

    #[test]
    fn maps_local_path_to_disk_and_legacy_remote_candidates() {
        let sync_root = PathBuf::from("/home/user/Yandex Disk");
        let local_path = PathBuf::from("/home/user/Yandex Disk/Docs/A.txt");
        let candidates = map_local_to_remote_candidates(&local_path, &sync_root).unwrap();
        assert_eq!(candidates[0], "disk:/Docs/A.txt");
        assert_eq!(candidates[1], "/Docs/A.txt");
    }

    #[test]
    fn rejects_path_outside_sync_root() {
        let sync_root = PathBuf::from("/home/user/Yandex Disk");
        let local_path = PathBuf::from("/home/user/Other/file.txt");
        let err = map_local_to_remote_candidates(&local_path, &sync_root).unwrap_err();
        assert!(matches!(err, ExtensionError::OutsideSyncRoot));
    }

    #[test]
    fn maps_remote_path_back_to_local_path() {
        let sync_root = PathBuf::from("/home/user/Yandex Disk");
        assert_eq!(
            map_remote_to_local_path("disk:/Docs/A.txt", &sync_root),
            PathBuf::from("/home/user/Yandex Disk/Docs/A.txt")
        );
        assert_eq!(
            map_remote_to_local_path("/Docs/B.txt", &sync_root),
            PathBuf::from("/home/user/Yandex Disk/Docs/B.txt")
        );
    }

    #[test]
    fn parses_partial_state_from_dbus() {
        assert_eq!(SyncUiState::from_dbus("partial"), SyncUiState::Partial);
        assert_eq!(SyncUiState::Partial.as_dbus(), "partial");
        assert_eq!(
            SyncUiState::Partial.badge_label(),
            "Partially available offline"
        );
    }
}
