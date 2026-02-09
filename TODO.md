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
- [x] Token storage: secret portal (Flatpak) + keyring fallback (classic).
- [x] OAuth helper: authorize URL + code обмен на токен.
- [x] OAuth flow + хранение токенов в Keyring/portal.

---

## 1) Документация (ссылки)
- Yandex Disk REST API: https://yandex.com/dev/disk/rest/
- Yandex Disk API overview: https://yandex.com/dev/disk/
- OAuth (Yandex): https://oauth.yandex.com/
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
- cloud_only → «cloud» эмблема.
- cached → «check»/«offline» эмблема.
- syncing → «sync» эмблема.
- error → «error» эмблема.

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

- Хранить токены только в GNOME Keyring (libsecret).
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
- `libsecret` bindings (токены)

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
