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
        unsafe extern "C" fn(provider: *mut NautilusMenuProvider, files: *mut GList) -> *mut GList,
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
    fn nautilus_file_info_add_emblem(file_info: *mut NautilusFileInfo, emblem_name: *const c_char);
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

include!("nautilus_plugin_runtime.rs");
