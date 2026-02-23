# TODO: Нативный GNOME‑клиент Yandex Disk (Rust, REST API, Flatpak)

## 0) Цели продукта и UX
- **Модель, близкая к iCloud Drive**: дерево файлов видно в Files (Nautilus), при этом файлы могут быть «в облаке» без локальной загрузки.
- **Двусторонняя синхронизация**: локальные изменения автоматически уходят в облако; изменения в облаке отражаются локально.
- **On‑demand download**: файл скачивается при открытии/копировании, либо по команде «Скачать/Сделать оффлайн».
- **Индикаторы состояния**: эмблемы/иконки состояния (cloud‑only / offline / syncing / error) прямо в Files.
- **Фоновый демон**: устойчивые очереди операций, retry/backoff, offline‑режим.
- **Flatpak‑first**: архитектура должна быть совместима с sandbox и требованиями Flathub.

---

## Статус реализации
- [x] Создан Rust workspace (yadisk-core, yadiskd).
- [x] REST API client: `get_disk_info`/`get_resource` + unit‑tests.
- [x] REST API client: download/upload link методы.
- [x] REST API client: list_directory с `_embedded.items`.
- [x] REST API client: create_folder (PUT /resources).
- [x] REST API client: move_resource (operation link).
- [x] REST API client: copy_resource (operation link).
- [x] REST API client: delete_resource (204/operation link).
- [x] REST API client: operation status (GET /operations/{id}).
- [x] Token storage: secret portal (Flatpak) + keyring fallback (classic).
- [x] OAuth helper: authorize URL + code обмен на токен.
- [x] OAuth flow + хранение токенов в Keyring/portal.
- [x] Sandbox policy for secret storage: в sandbox не делать неявный fallback в keyring при недоступном portal.
- [x] Sync engine: базовая очередь операций (TDD).
- [x] Sync engine: стратегия разрешения конфликтов (three‑way, keep‑both).
- [x] Sync engine: SQLite индекс (items/states).
- [x] Sync engine: sync cursor persistence.
- [x] Sync engine: ops_queue persistence.
- [x] Sync engine: backoff helper (exponential + jitter).
- [x] Resource metadata: `resource_id`/`md5` fields in REST deserialization.
- [x] Sync engine: cache path mapping helper (`cache_path_for`).
- [x] Sync engine: transfer client (download via transfer link).
- [x] Sync engine: transfer client (upload via transfer link).
- [x] Sync engine: runnable engine skeleton (`SyncEngine`).
- [x] Sync engine: remote directory indexing (`sync_directory_once`).
- [x] Sync engine: persisted op enqueue for download/upload.
- [x] Sync engine: execute download/upload ops and mark cached.
- [x] Sync engine: operation polling helper (wait with backoff).
- [x] Sync engine: conflicts persistence (record/list).
- [x] OAuth storage: full OAuth state JSON (`access/refresh/expires/scope/type`) + legacy plain-token migration.
- [x] OAuth client: refresh-token flow + tests.
- [x] Daemon token provider: выдача валидного access token с auto-refresh.
- [x] Daemon startup: retry одного API-запроса после refresh при `401`.
- [x] Yadisk client: `fields=` support + `list_directory_all`.
- [x] Incremental cloud sync strategy: polling+diff с тестами, включая recursive index и rename/delete handling.
- [x] Yadisk API errors: typed classification (auth/rate-limit/transient/permanent).
- [x] Transfer client: atomic download, streaming upload/download, md5 check, concurrency limits.
- [x] SQLite migrations: переход на `sqlx` migrations + upgrade test для legacy schema.
- [x] SQLite index: `parent_path`, expanded sync-state fields, baseline fields, default XDG data DB path helper.
- [x] Ops queue: payload JSON, scheduling (`attempt/retry_at/priority`), dedupe/coalescing, requeue helper.
- [x] D-Bus API (zbus): сервисные методы/сигналы + mapping ошибок на стабильные D-Bus error names (TDD).
- [x] Sync engine: incremental recursive cloud sync + rename/delete reconciliation.
- [x] Sync engine: notify-based local event source + enqueue upload/move/delete.
- [x] Sync engine: conflict pipeline integration + conflict persistence on KeepBoth.
- [x] Sync engine: async move/delete operation handling with completion polling + index updates.
- [x] Sync testing: e2e sync loop + retry edge-cases + conflict transition tests.
- [x] FUSE crate: `yadisk-fuse` c `readdir/getattr`, on-demand read enqueue, write/rename/delete/mkdir/rmdir enqueue, xattr state constant.
- [x] GNOME integration MVP scaffolds: `yadisk-integrations` (Nautilus actions/emblems + libcloudproviders account/status model, Adwaita icons).
- [x] Packaging artifacts: systemd user units, Flatpak manifest skeleton, host package matrix file.

---

## Что осталось сделать (полный TODO, максимально подробно)

> Ниже — только **оставшиеся** задачи. Уже реализованное отмечено в «Статус реализации».

### A) Зафиксировать целевую Flathub-модель (архитектурное решение)
- [x] Зафиксировать один основной сценарий доставки:
  - [x] **A1 (host-helper)**: Flatpak UI + отдельный host daemon/FUSE пакет (deb/rpm/arch).
  - [x] **A2 (sandbox-only)**: без FUSE, только sandbox cache + UI-обзор файлов.
- [x] Для A1: формализовать контракт sandbox ↔ host daemon:
  - [x] D-Bus интерфейс (name/path/methods/signals/errors).
  - [x] Правила запуска/обнаружения host-компонента из sandbox.
  - [x] Политика доступа к sync-папке без широких filesystem permissions.
- [x] Для A2: определить UX (что именно пользователь видит в Files и что только в приложении).

### B) OAuth и токены (production-ready)
- [x] Перейти от хранения `access_token`-строки к хранению полного OAuth-состояния:
  - [x] `access_token`, `refresh_token`, `expires_at`, `scope`, `token_type`.
  - [x] Сериализация в JSON и хранение через текущий `TokenStorage` (portal/keyring).
  - [x] Миграция старого формата токена (plain string) в новый.
- [x] Добавить refresh-token flow в `yadisk-core::OAuthClient` + wiremock tests.
- [x] В daemon добавить token provider:
  - [x] выдача валидного access token с auto-refresh.
  - [x] retry одного запроса после refresh на `401`.

### C) `yadisk-core`: довести API слой до нужд синка
- [x] Реализовать удобный paging helper (`list_directory_all`) до `total`.
- [x] Добавить `fields=` поддержку для тяжелых запросов (снижение payload).
- [x] Добавить endpoint(ы)/стратегию инкрементальных изменений (cursor/token или polling+diff) и покрыть тестами.
- [x] Типизировать классы API-ошибок для retry policy (auth/rate-limit/transient/permanent).

### D) Transfer слой (большие файлы, атомарность, контроль целостности)
- [x] Download делать атомарно: `*.partial` + rename по завершению.
- [x] Upload/Download перевести на streaming (без чтения всего файла в память).
- [x] Проверка целостности после download (md5, где доступно в metadata).
- [x] Лимиты параллелизма upload/download + конфиг.

### E) SQLite индекс и миграции
- [x] Перевести схему на `sqlx` migrations (`migrations/`) вместо hardcoded SQL.
- [x] Добавить поля для полноценного sync-state:
  - [x] `retry_at`, `last_success_at`, `last_error_at`, `dirty`.
  - [x] baseline-поля для three-way конфликтов (`last_synced_hash` / `last_synced_modified`).
- [x] Добавить parent relation (`parent_path` или `parent_id`) для дерева и ускорения readdir.
- [x] Зафиксировать реальный путь БД (XDG data dir) и инициализацию директорий.

### F) Ops queue: payload, retries, дедупликация
- [x] Расширить `ops_queue` payload (JSON) для move/rename/copy/delete параметров.
- [x] Добавить scheduling: `attempt`, `retry_at`, `priority`.
- [x] Реализовать requeue с backoff и классификацией ошибок (transient/permanent).
- [x] Добавить дедупликацию/coalescing операций по path/item.

### G) `SyncEngine`: довести до полного двустороннего цикла
- [x] Cloud→Local:
  - [x] рекурсивная индексация дерева + pagination.
  - [x] корректная обработка удалений/переименований из облака.
  - [x] auto-download для pinned файлов.
- [x] Local→Cloud:
  - [x] источник локальных событий (FUSE hooks или `notify` watcher).
  - [x] enqueue upload/move/delete по локальным изменениям.
- [x] Интегрировать `sync::conflict` в реальный pipeline:
  - [x] вычисление base/local/remote.
  - [x] `KeepBoth` rename + запись в `conflicts`.
- [x] Для async операций REST (move/copy/delete) — хранить operation URL, ждать completion, обновлять state.

### H) D-Bus API daemon ↔ интеграции
- [x] Реализовать zbus сервис с методами:
  - [x] `Download(path)`, `Pin(path,bool)`, `Evict(path)`, `Retry(path)`, `GetState(path)`, `ListConflicts()`.
- [x] Реализовать сигналы:
  - [x] `StateChanged(path,state)`, `ConflictAdded(id,path,renamed_local)`.
- [x] Прописать и покрыть тестами mapping ошибок в D-Bus error names.

### I) FUSE слой (для host-helper варианта)
- [x] Создать отдельный crate (например `yadisk-fuse`) и реализовать:
  - [x] `readdir/getattr` из `IndexStore`.
  - [x] `open/read` с on-demand download.
  - [x] `write/flush/rename/unlink/mkdir/rmdir` с enqueue соответствующих ops.
- [x] Добавить xattr состояние (`user.yadisk.state`) для эмблем.
- [x] Интеграционные FUSE-тесты (gated в CI).

### J) GNOME интеграция (Nautilus + libcloudproviders)
- [x] Зафиксировать host-side модель интеграции для Files (вариант A1): extension/provider ставятся в host и работают с `yadiskd` по D-Bus.
- [x] Nautilus extension:
  - [x] MVP-скелет extension (info provider + menu provider) и подключение к D-Bus API демона.
  - [x] Native host extension crate `yadisk-nautilus` (`cdylib`) c D-Bus proxy, state-aware menu/action model и signal listener.
  - [x] эмблемы cloud/offline/syncing/error.
  - [x] Для MVP использовать стандартные Adwaita symbolic icons (без собственного icon theme/asset pack).
  - [x] контекстные действия Download/Pin/Evict/Retry.
  - [x] Scripts-only подход переведён в fallback/legacy; основной путь интеграции — нативный Rust extension.
  - [x] подписка на D-Bus сигналы для live updates.
- [x] libcloudproviders provider:
  - [x] экспорт account/provider в sidebar Files.
  - [x] привязка к локальной sync точке и статусам.
  - [x] синхронизация account health/state c `yadiskd` (online/offline/error).

#### J.1 Полная нативная интеграция Nautilus (GNOME 49+)
- [x] Реальный host extension (не scripts-only), загружаемый Nautilus как расширение.
  - [x] Зафиксирован baseline: только свежий GNOME/Nautilus (49+), без обратной совместимости со старыми ABI.
  - [x] Реализация extension целиком на Rust (`yadisk-nautilus`) через `cdylib` и `libnautilus-extension` (API 4.1/50.rc, GNOME 49+ сигнатуры).
  - [x] `InfoProvider`: состояние файла через D-Bus `GetState`, эмблемы cloud/cached/syncing/error.
  - [x] `MenuProvider`: state-aware контекстные действия (`Save Offline`, `Download Now`, `Remove Offline Copy`, `Retry Sync`).
  - [x] Вызовы действий в `yadiskd` по D-Bus (`Pin`, `Download`, `Evict`, `Retry`) с fallback путей `disk:/...` и `/...`.
  - [x] Live updates: подписка на D-Bus сигналы `StateChanged`/`ConflictAdded` и инвалидация extension info.
  - [x] Установка в host-путь расширений Nautilus + smoke-проверка загрузки и вызовов.

### K) Service management и packaging
- [x] systemd --user units:
  - [x] `yadiskd.service` (restart policy, deps, logging).
  - [x] (A1) отдельный unit для FUSE mount lifecycle.
- [x] Подготовить Flatpak manifest (если будет UI crate):
  - [x] portal permissions (Secret/OpenURI), сеть, без лишних filesystem access.
- [x] Для A1: подготовить хост-пакеты (deb/rpm/arch) для daemon/FUSE/extension/provider.

### L) Тестирование и quality gates (что ещё не покрыто)
- [x] Добавить e2e-тест «sync loop» (cloud list -> enqueue -> transfer -> index state transitions).
- [x] Добавить тесты retry/requeue и edge-cases очереди (permanent failure, max attempts).
- [x] Добавить тесты конфликтов на реальных переходах состояния в `SyncEngine`.
- [x] Добавить тесты миграций БД (upgrade from previous schema).
- [x] Добавить smoke-test сценарии sandbox/portal и host-helper режимов в CI (по возможности).

### M) Реализация полноценного демона (осталось сделать)
- [x] **D-Bus сервис** — запуск zbus сервера в `main.rs` для обработки запросов от интеграций
  - [x] Интегрировать `SyncDbusService` из `dbus_api.rs`
  - [x] Регистрация D-Bus имени `com.yadisk.Sync1`
  - [x] Обработка методов: Download, Pin, Evict, Retry, GetState, ListConflicts
  - [x] Отправка сигналов: StateChanged, ConflictAdded
- [x] **Фоновый цикл синхронизации** — бесконечный цикл в daemon'е:
  - [x] Polling изменений в облаке (polling+diff strategy)
  - [x] Обработка локальных событий через `notify` watcher
  - [x] Выполнение операций из очереди с backoff/retry
  - [x] Управление кэшем (LRU eviction)
- [x] **systemd unit** — правильная конфигурация:
  - [x] `yadiskd.service` с restart policy и dependencies
  - [x] `yadiskd.path` для auto-restart при изменении токена
  - [x] Логирование через journald
- [x] Подготовлен host-side smoke script для GNOME/D-Bus проверки: `packaging/host/gnome-live-smoke.sh`.

## 1) Документация (ссылки)
- Yandex Disk REST API: https://yandex.ru/dev/disk/rest/
- Yandex Disk API overview: https://yandex.ru/dev/disk/
- OAuth (Yandex): https://oauth.yandex.ru/
- libcloudproviders (GNOME): https://gnome.pages.gitlab.gnome.org/libcloudproviders/index.html
- Nautilus Extension API: https://gnome.pages.gitlab.gnome.org/nautilus/
- FUSE (Linux kernel docs): https://www.kernel.org/doc/html/latest/filesystems/fuse.html
- Flatpak Portals overview: https://docs.flatpak.org/en/latest/portal-api-reference.html

---

## 2) Архитектура (компоненты)

### 2.1 Демон синхронизации (Rust, systemd --user)
**Ответственность:**
- OAuth авторизация и обновление токена (через OAuth code flow + loopback redirect, либо локальный callback сервер).
- Двусторонняя синхронизация: cloud → local и local → cloud.
- Очереди операций (upload/download/rename/delete), ретраи, дедупликация, конфликт‑резолвинг.
- Локальный индекс метаданных и состояний.
- Управление кэшем и политиками оффлайн/онлайн.

**Ключевые подсистемы:**
- **Sync Engine**: планировщик задач + worker pool (tokio).
- **Remote API Client**: Yandex REST API (reqwest + serde).
- **Local Index**: SQLite (sqlx) с транзакциями.
- **File State Manager**: состояния файлов (cloud‑only, cached, syncing, error).
- **Event Sources**:
  - Поллинг cloud‑изменений (endpoint изменений/листинг по timestamps).
  - inotify/notify для локальных изменений.

### 2.2 FUSE‑FS (Rust, fuser)
**Ответственность:**
- Показывать дерево файлов в локальной точке монтирования (~/YandexDisk).
- Placeholder‑файлы: отображать размер/метаданные без загрузки контента.
- При чтении — триггерить on‑demand download через демона.
- При записи — создавать локальный файл, ставить флаг на загрузку в облако.
- Xattrs для хранения состояния (например, `user.yadisk.state`, `user.yadisk.etag`).

### 2.3 Nautilus Extension (Rust/GObject) + Emblems
**Ответственность:**
- Добавлять эмблемы к файлам по их состоянию (cloud‑only/offline/syncing/error).
- Добавлять контекстное меню: «Скачать», «Сделать оффлайн», «Освободить место», «Повторить синхронизацию».
- Передавать команды демону по D‑Bus.

### 2.4 libcloudproviders Provider
**Ответственность:**
- Регистрация провайдера в GNOME Files (боковая панель, account‑presence).
- Экспорт статусов аккаунта/синхронизации (DBus). 

### 2.5 UI/Settings (опционально)
- Минимальный GTK4‑UI: статус синхронизации, выбор локальной папки, политика кэша, логин/выход.

---

## 3) Flatpak + Portals (ключевой раздел)

### 3.1 Ограничения Flatpak
- Flatpak sandbox по умолчанию **не позволяет** монтирование FUSE внутри контейнера.
- Доступ к домашней папке ограничен; для работы с файлами предпочтительны **portals**.
- Nautilus extension и libcloudproviders обычно живут **в хост‑системе**, что осложняет поставку как единого Flatpak.

### 3.2 Возможные варианты, совместимые с Flathub

**Вариант A (рекомендуемый):**
- **Вынести FUSE + демон за пределы контейнера** (host service) и общаться с ним через D‑Bus.
- Flatpak‑приложение — UI/настройки, OAuth‑логин, контроль сервиса.
- Хост‑сервис устанавливается отдельным пакетом (rpm/deb/arch), не через Flatpak.
- Плюс: полноценный FUSE и интеграция в Files.
- Минус: сложнее распространять и пройти Flathub (двойной пакет).

**Вариант B (Sandbox‑first, порталы):**
- Отказ от FUSE в Flatpak. Использовать **FileChooser/Document portals** для доступа к файлам пользователя.
- Реализовать **внутренний кэш** в sandbox (`~/.var/app/.../cache`) и показывать каталог через UI приложения, а не как системную папку.
- Плюс: соответствует Flathub требованиям.
- Минус: нет нативной папки в Files, UX хуже.

**Вариант C (Portal + GVfs):**
- Использовать существующие gvfs‑механизмы (если бы был backend) — однако кастомный gvfs backend нельзя запустить из Flatpak без хост‑компонента.
- На практике сводится к варианту A.

### 3.3 Вывод по Flatpak
- **Полностью нативная интеграция с Files (FUSE + эмблемы)** требует **хост‑компонента**, что выходит за чистый Flatpak.
- Для Flathub‑совместимости нужно разделить: UI/настройки в Flatpak, системный daemon/FUSE как отдельный install.

---

## 4) Yandex Disk REST API: базовые операции

**OAuth:**
- Получение access_token через OAuth code flow.
- Хранение токена в GNOME Keyring (libsecret) + refresh (если предусмотрен).

**Основные вызовы (ориентир):**
- **GET /v1/disk** — информация о диске/квоте.
- **GET /v1/disk/resources?path=...** — метаданные файла/папки.
- **GET /v1/disk/resources?path=...&limit=...&offset=...** — листинг.
- **GET /v1/disk/resources/download?path=...** — URL для скачивания.
- **GET /v1/disk/resources/upload?path=...&overwrite=true** — URL для загрузки.
- **POST /v1/disk/resources** — создание папок.
- **POST /v1/disk/resources/move** — перемещение.
- **POST /v1/disk/resources/copy** — копирование.
- **DELETE /v1/disk/resources** — удаление.
- **Изменения**: использовать официальный endpoint изменений/поллинга (если доступен) или fallback через периодическое сравнение списков.

**Метаданные:**
- Хранить `path`, `name`, `type`, `size`, `modified`, `md5`/`sha256` (если есть), `resource_id`.

---

## 5) Локальная модель данных (SQLite)

### Таблицы
- **items**: id, path, parent_id, name, type(file/dir), size, mtime, remote_hash, remote_resource_id.
- **states**: item_id, state (cloud_only/cached/syncing/error), pin (bool), last_error, retry_at.
- **ops_queue**: op_id, item_id, kind(upload/download/rename/delete/mkdir), payload, priority, attempts.
- **sync_cursor**: last_sync_time / cursor_token (если API поддерживает).
- **cache**: item_id, local_path, size, last_accessed.

### Индексация
- Индекс по `path`, `parent_id`.
- Индекс по `state` и `retry_at` для фоновых задач.

---

## 6) Состояния файлов и UX

**Состояния:**
- `cloud_only`: файл существует в облаке, локального контента нет.
- `cached`: файл скачан, доступен оффлайн.
- `syncing`: идет upload/download.
- `error`: ошибка синка.

**Эмблемы (Nautilus):**
- cloud_only → стандартная cloud‑эмблема Adwaita.
- cached → стандартная check/offline‑эмблема Adwaita.
- syncing → стандартная sync‑эмблема Adwaita.
- error → стандартная error‑эмблема Adwaita.

**Контекстные действия:**
- «Скачать» → enqueue download + pin.
- «Сделать оффлайн» → pin + download.
- «Освободить место» → удаление локального кэша, но keep cloud.
- «Повторить» → retry.

---

## 7) FUSE‑FS детали

### Основные операции
- **getattr**: возвращать размер и mtime из локальной БД даже для cloud_only.
- **readdir**: список из локального индекса (синхронизированного демоном).
- **open/read**: если cloud_only → запрос демону на download, ожидание/streaming.
- **write/flush/release**: сохранить в локальном кэше, поставить op upload.
- **unlink/rmdir/rename**: локально пометить + enqueue REST операцию.

### Placeholder стратегия
- **Размер**: использовать реальный размер файла из облака для корректных UI‑индикаторов.
- **Контент**: пустой файл в кеше до первого чтения.
- **xattr**: хранить состояние (cloud_only/cached).

### Кэширование
- Локальный кэш в `~/.cache/yadisk/` с LRU‑очисткой.
- Порог по объему (настройка пользователя).

---

## 8) Sync Engine (двусторонний)

### Cloud → Local
- Периодический polling изменений.
- Если API предоставляет cursor/token — использовать для инкрементального синка.
- При обнаружении изменения:
  - Обновить metadata в БД.
  - Если file pinned → auto‑download.
  - Если cloud_only → только metadata.

### Local → Cloud
- inotify/notify для локальных изменений.
- Изменения записывать в ops_queue.
- Для крупных файлов: multipart/chunk upload (если доступен).

### Конфликты
- Сравнение `mtime` и `hash`.
- При конфликте: локальное переименование с суффиксом ("conflict").
- Логи конфликта в отдельной таблице.

### Ретраи и backoff
- Экспоненциальный backoff + jitter.
- Различать transient и permanent ошибки.

---

## 9) IPC / D‑Bus

### DBus сервис демона
- Методы: `Download(path)`, `Pin(path)`, `Unpin(path)`, `Retry(path)`, `GetStatus(path)`.
- Сигналы: `StateChanged(path, state)`.

### Использование
- Nautilus extension вызывает DBus методы.
- UI‑приложение (GTK4) использует тот же DBus.

---

## 10) libcloudproviders

- Экспорт провайдера и аккаунтов через DBus интерфейсы:
  - `org.freedesktop.CloudProviders.Provider`
  - `org.freedesktop.CloudProviders.Account`
- При логине создать account и указать локальную папку.

---

## 11) Безопасность

- Хранить токены через `TokenStorage`: Secret portal (предпочтительно в sandbox) + keyring fallback вне strict sandbox.
- Не писать токены в логи.
- TLS‑валидация по умолчанию.

---

## 12) Rust‑стек (черновой)

- `tokio`, `reqwest`, `serde`, `serde_json`
- `sqlx` (SQLite)
- `fuser` (FUSE)
- `notify` (inotify abstraction)
- `xattr` (extended attributes)
- `zbus` (DBus)
- `glib`, `gio` (для GNOME интеграции)
- `ashpd` (OpenURI/Secret portal), `keyring` (fallback storage)

---

## 13) Сервисы и упаковка

- systemd --user:
  - `yadiskd.service` (демон)
  - `yadiskd.path` (опционально)
- Flatpak manifest:
  - Разрешения на D‑Bus, сеть, portal access.
  - Ограничить доступ к файловой системе.

---

## 14) Тестирование

- Использовать TDD: сначала тест/ожидаемое поведение, затем реализация.
- Unit‑тесты API‑клиента (mock HTTP).
- Интеграционные тесты sync engine (фейковый REST сервер).
- FUSE‑тесты (mount in temp dir + fs operations).
- DBus‑тесты (zbus + mock service).
- Перед PR/релизом: `cargo fmt` и `cargo clippy -- -D warnings`.

---

## 15) Миграции/MVP этапы

1) MVP‑1: OAuth + REST API client + листинг + download.
2) MVP‑2: FUSE mount + placeholder + on‑demand download.
3) MVP‑3: двусторонний sync + queue + конфликт‑резолвинг.
4) MVP‑4: Nautilus extension + эмблемы + контекстное меню.
5) MVP‑5: libcloudproviders + UI settings.
6) MVP‑6: Flatpak packaging + host‑helper strategy.
