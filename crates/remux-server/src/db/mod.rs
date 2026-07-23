use anyhow::Result;
use serde::{Deserialize, Serialize};
use sqlx::{
    ConnectOptions as _, SqlitePool,
    sqlite::{
        SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous,
    },
};
use std::{str::FromStr, time::Duration};
use tracing::{info, warn};
use uuid::Uuid;
pub mod api_key;
pub mod auth;
pub mod image;
pub mod iptv;
pub mod media;
pub mod settings;
pub mod stream_group;
pub mod task;
pub mod user;
pub use api_key::*;
pub use image::*;
pub use iptv::*;
pub use media::*;
pub use settings::*;
pub use stream_group::*;
pub use task::*;
pub use user::*;

pub async fn connect(url: &str, slow_query_threshold_ms: u64) -> Result<SqlitePool> {
    let opts = SqliteConnectOptions::from_str(url)?
        .journal_mode(SqliteJournalMode::Wal)
        .synchronous(SqliteSynchronous::Normal)
        .pragma("wal_autocheckpoint", "1000")
        .pragma("cache_size", "-16384")
        .pragma("mmap_size", "33554432")
        .pragma("temp_store", "memory")
        // Allow up to 10s of retrying when blocked by another connection's
        // write lock. This is what makes wal_checkpoint(TRUNCATE) actually
        // wait for in-flight reads to finish instead of giving up immediately.
        .busy_timeout(Duration::from_secs(10))
        .log_slow_statements(
            log::LevelFilter::Warn,
            Duration::from_millis(slow_query_threshold_ms),
        );
    Ok(SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(opts)
        .await?)
}

/// Whether `err` is SQLite's "database is busy/locked" error (writer lock
/// contention), as opposed to a definite failure (constraint violation, bad
/// SQL, etc.) that would just fail the same way again on retry.
///
/// Matches on the *primary* result code (masking off the extended code bits)
/// since `sqlite3_extended_errcode` can return e.g. `SQLITE_BUSY_TIMEOUT`
/// (773) or `SQLITE_BUSY_SNAPSHOT` (517), not just plain `SQLITE_BUSY` (5).
pub(crate) fn is_sqlite_busy_or_locked(err: &sqlx::Error) -> bool {
    const SQLITE_BUSY: i32 = 5;
    const SQLITE_LOCKED: i32 = 6;
    let sqlx::Error::Database(db_err) = err else {
        return false;
    };
    db_err
        .code()
        .and_then(|c| {
            c.parse::<i32>()
                .ok()
        })
        .map(|code| matches!(code & 0xFF, SQLITE_BUSY | SQLITE_LOCKED))
        .unwrap_or(false)
}

/// Retry `f` up to `attempts` times (exponential backoff + jitter-free delay
/// doubling, base `delay_ms`) when it fails with [`is_sqlite_busy_or_locked`].
///
/// `busy_timeout` (see [`connect`]) already makes SQLite itself wait up to
/// 10s before surfacing `SQLITE_BUSY`; this is a belt-and-suspenders layer
/// for the rare case where even that isn't enough under heavy concurrent
/// write load. Non-busy errors (constraint violations, bad SQL, etc.) are
/// returned immediately without retrying, since they won't succeed on a
/// second attempt.
pub(crate) async fn retry_on_busy<F, Fut, T>(
    attempts: u32,
    delay_ms: u64,
    mut f: F,
) -> Result<T, sqlx::Error>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, sqlx::Error>>,
{
    let mut last_err = None;
    for attempt in 0..attempts.max(1) {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                let retryable = is_sqlite_busy_or_locked(&e);
                last_err = Some(e);
                if !retryable || attempt + 1 >= attempts {
                    break;
                }
                let backoff_ms = delay_ms.saturating_mul(1u64 << attempt.min(10));
                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
            }
        }
    }
    Err(last_err.expect("loop runs at least once"))
}

const LAST_PRE_SQUASH: i64 = 202606140004; // last migration on main before this PR
const SQUASH_VERSION: i64 = 202606140005; // squash migration version

async fn prepare_squash(pool: &SqlitePool) -> Result<()> {
    let last: Option<i64> = sqlx::query_scalar(
        "SELECT version FROM _sqlx_migrations \
         WHERE success = TRUE ORDER BY version DESC LIMIT 1",
    )
    .fetch_optional(pool)
    .await
    .ok()
    .flatten();

    match last {
        None => {}
        Some(v) if v < LAST_PRE_SQUASH => anyhow::bail!(
            "Database schema is outdated. Please update to remux v0.8.0 first, \
             then upgrade to this version."
        ),
        Some(v) if v < SQUASH_VERSION => {
            sqlx::query("DELETE FROM _sqlx_migrations")
                .execute(pool)
                .await?;
        }
        Some(_) => {
            // The squash migration may be edited (e.g. to update seed data).
            // Patch the stored checksum to match the current file so sqlx
            // accepts it without re-executing the migration.
            if let Some(m) = sqlx::migrate!("./migrations")
                .migrations
                .iter()
                .find(|m| m.version == SQUASH_VERSION)
            {
                sqlx::query(
                    "UPDATE _sqlx_migrations SET checksum = ? WHERE version = ?",
                )
                .bind(
                    m.checksum
                        .as_ref(),
                )
                .bind(SQUASH_VERSION)
                .execute(pool)
                .await?;
            }
        }
    }
    Ok(())
}

pub async fn migrate(pool: &SqlitePool) -> Result<()> {
    prepare_squash(pool).await?;
    sqlx::migrate!("./migrations")
        .run(pool)
        .await?;

    migrate_channel_ids(pool).await?;
    vacuum_if_needed(pool).await?;
    // Ensure query-planner statistics are fresh on every startup. PRAGMA optimize
    // only re-analyzes tables/indexes where stats are significantly out of date,
    // so it is fast on subsequent startups and repairs any stale stats from
    // installs that pre-date the per-task PRAGMA optimize.
    sqlx::query("PRAGMA optimize")
        .execute(pool)
        .await?;
    Ok(())
}

async fn vacuum_if_needed(pool: &SqlitePool) -> Result<()> {
    let freelist: i64 = sqlx::query_scalar("PRAGMA freelist_count")
        .fetch_one(pool)
        .await
        .unwrap_or(0);
    if freelist > 50_000 {
        info!(
            freelist_pages = freelist,
            "vacuuming database to reclaim freed pages"
        );
        let mut conn = pool
            .acquire()
            .await?;
        // VACUUM's internal sort operations use temp storage. The pool uses
        // temp_store=memory for query performance, but that causes OOM on large
        // databases during VACUUM. Switch to file-backed temp for this operation.
        sqlx::query("PRAGMA temp_store = 1")
            .execute(&mut *conn)
            .await?;
        sqlx::query("VACUUM")
            .execute(&mut *conn)
            .await?;
        sqlx::query("PRAGMA temp_store = 2")
            .execute(&mut *conn)
            .await?;
    }
    Ok(())
}

async fn backfill_certification_age(pool: &SqlitePool) -> Result<()> {
    let config = Settings::get_config_or_default(pool).await;
    let rows = sqlx::query_as::<_, (uuid::Uuid, String)>(
        "SELECT id, certification FROM media WHERE certification IS NOT NULL AND certification_age IS NULL",
    )
    .fetch_all(pool)
    .await?;

    for (id, certification) in rows {
        if let Some(age) = crate::localization::ratings::resolve_rating_age(
            Some(&certification),
            config
                .metadata_country_code
                .as_deref(),
        ) {
            sqlx::query("UPDATE media SET certification_age = ?1 WHERE id = ?2")
                .bind(age)
                .bind(id)
                .execute(pool)
                .await?;
        }
    }

    Ok(())
}

/// One-time startup migration: re-key all `tv_channel` rows from the old
/// tvg-id/name-based UUID scheme to a URL-based UUID scheme (matching Jellyfin).
///
/// The old scheme collapsed channels that shared a `tvg-id` (e.g. SD/HD/4K
/// variants) into one row.  The new scheme seeds the UUID on the stream URL so
/// every distinct URL gets its own channel, preserving user settings across the
/// scheme change instead of wiping them on the next IPTV refresh.
///
/// Runs at most once (guarded by the `channel_id_scheme_v2_migrated` settings
/// key).  For ~20 k channels the migration completes in under 20 seconds on
/// slow hardware.
pub async fn migrate_channel_ids(pool: &SqlitePool) -> Result<()> {
    if Settings::get(pool, "channel_id_scheme_v2_migrated")
        .await?
        .is_some()
    {
        return Ok(());
    }

    // Fetch every tv_channel that has both an iptv_source_id and a stream URL.
    // Channels missing either cannot be re-keyed and are left unchanged.
    #[derive(sqlx::FromRow)]
    struct Row {
        id: Uuid,
        title: String,
        tvg_id: Option<String>,
        iptv_source_id: String,
        stream_url: String,
    }
    let rows = sqlx::query_as::<_, Row>(
        "SELECT
             id,
             title,
             tvg_id,
             json_extract(external_ids, '$.iptv_source_id') AS iptv_source_id,
             json_extract(stream_info,  '$.descriptor.Http.url') AS stream_url
         FROM media
         WHERE kind = 'tv_channel'
           AND json_extract(external_ids, '$.iptv_source_id') IS NOT NULL
           AND json_extract(stream_info,  '$.descriptor.Http.url') IS NOT NULL
           AND json_extract(stream_info,  '$.descriptor.Http.url') != ''",
    )
    .fetch_all(pool)
    .await?;

    if rows.is_empty() {
        Settings::set(pool, "channel_id_scheme_v2_migrated", "1").await?;
        return Ok(());
    }

    // Compute (old_uuid, new_uuid) pairs.
    let mut pairs: Vec<(Uuid, Uuid)> = Vec::with_capacity(rows.len());
    for row in &rows {
        let Ok(addon_id) = Uuid::try_parse(&row.iptv_source_id) else {
            continue;
        };
        let tvg_key = row
            .tvg_id
            .as_deref()
            .unwrap_or(&row.title);
        let old_id = Uuid::new_v5(&addon_id, tvg_key.as_bytes());
        // Skip rows that don't match the expected old UUID — they were already
        // migrated or created by a different scheme.
        if old_id != row.id {
            continue;
        }
        let new_id = Uuid::new_v5(
            &addon_id,
            row.stream_url
                .as_bytes(),
        );
        if old_id == new_id {
            continue;
        }
        pairs.push((old_id, new_id));
    }

    if pairs.is_empty() {
        Settings::set(pool, "channel_id_scheme_v2_migrated", "1").await?;
        return Ok(());
    }

    info!(
        count = pairs.len(),
        "migrating IPTV channel UUIDs to URL-based scheme"
    );

    // Acquire a dedicated connection: PRAGMA foreign_keys is connection-scoped.
    let mut conn = pool
        .acquire()
        .await?;
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(&mut *conn)
        .await?;

    let result: Result<()> = async {
        // Process 200 channels per batch to stay under SQLite's 999-param limit.
        // Each batch fires 5 CASE-WHEN UPDATE statements (one per FK column) plus
        // a WAL checkpoint to keep WAL size bounded during large migrations.
        const BATCH: usize = 200;
        for chunk in pairs.chunks(BATCH) {
            sqlx::query("BEGIN IMMEDIATE")
                .execute(&mut *conn)
                .await?;

            // Helper: build and execute one CASE-WHEN UPDATE for a given table/column.
            // UPDATE <table> SET <col> = CASE <col> WHEN old THEN new ... END WHERE <col> IN (...)
            macro_rules! rename_col {
                ($table:expr, $col:expr) => {{
                    let mut sql =
                        format!("UPDATE {} SET {} = CASE {}", $table, $col, $col);
                    for _ in chunk {
                        sql.push_str(" WHEN ? THEN ?");
                    }
                    sql.push_str(" END WHERE ");
                    sql.push_str($col);
                    sql.push_str(" IN (");
                    for i in 0..chunk.len() {
                        if i > 0 {
                            sql.push(',');
                        }
                        sql.push('?');
                    }
                    sql.push(')');

                    let mut q = sqlx::query(&sql);
                    for (old, new) in chunk {
                        q = q
                            .bind(old)
                            .bind(new);
                    }
                    for (old, _) in chunk {
                        q = q.bind(old);
                    }
                    q.execute(&mut *conn)
                        .await?;
                }};
            }

            // Child FK columns first, then the PK itself.
            rename_col!("media", "parent_id");
            rename_col!("media_relations", "right_media_id");
            rename_col!("media_images", "media_id");
            rename_col!("media_tags", "media_id");
            rename_col!("media", "id");

            sqlx::query("COMMIT")
                .execute(&mut *conn)
                .await?;

            sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
                .execute(&mut *conn)
                .await
                .ok();
        }
        Ok(())
    }
    .await;

    // Always restore FK enforcement before returning.
    sqlx::query("PRAGMA foreign_keys = ON")
        .execute(&mut *conn)
        .await
        .ok();

    result?;

    Settings::set(pool, "channel_id_scheme_v2_migrated", "1").await?;
    info!("IPTV channel UUID migration complete");
    Ok(())
}

pub async fn checkpoint_db(pool: &SqlitePool) {
    sqlx::query("PRAGMA wal_checkpoint(FULL)")
        .execute(pool)
        .await;
}

#[derive(
    Copy,
    Serialize,
    Debug,
    Clone,
    Eq,
    PartialEq,
    Deserialize,
    Hash,
    strum_macros::Display,
    strum_macros::EnumString,
)]
#[serde(rename_all = "PascalCase")]
pub enum SortOrder {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum ScrollDirection {
    Horizontal,
    Vertical,
}

pub struct FilterResult<T> {
    pub records: Vec<T>,
    pub total_count: usize,
}

trait QueryBuilderExt<'q> {
    fn push_in<T>(&mut self, column: &str, values: &'q Vec<T>)
    where
        T: Send
            + Sync
            + for<'a> sqlx::Encode<'a, sqlx::Sqlite>
            + sqlx::Type<sqlx::Sqlite>
            + 'q;
}

impl<'q> QueryBuilderExt<'q> for sqlx::QueryBuilder<'q, sqlx::Sqlite> {
    fn push_in<T>(&mut self, column: &str, values: &'q Vec<T>)
    where
        T: Send
            + Sync
            + for<'a> sqlx::Encode<'a, sqlx::Sqlite>
            + sqlx::Type<sqlx::Sqlite>
            + 'q,
    {
        if values.is_empty() {
            return;
        };

        self.push(" AND ");
        self.push(column);
        self.push(" IN (");

        let mut separated = self.separated(", ");
        for v in values {
            separated.push_bind(v);
        }

        self.push(")");
    }
}

#[cfg(test)]
mod busy_retry_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn is_sqlite_busy_or_locked_false_for_other_errors() {
        assert!(!is_sqlite_busy_or_locked(&sqlx::Error::RowNotFound));
    }

    #[tokio::test]
    async fn retry_on_busy_does_not_retry_non_busy_errors() {
        let attempts = AtomicUsize::new(0);
        let result: Result<(), sqlx::Error> = retry_on_busy(3, 1, || {
            attempts.fetch_add(1, Ordering::SeqCst);
            async { Err(sqlx::Error::RowNotFound) }
        })
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn retry_on_busy_retries_configured_attempts_on_real_sqlite_busy() {
        // Two single-connection pools sharing one on-disk DB file, both with
        // busy_timeout=0 so contention surfaces immediately as SQLITE_BUSY
        // instead of after SQLite's internal wait.
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("busy_test.sqlite");
        let url = format!("sqlite://{}?mode=rwc", path.display());

        async fn connect_single(url: &str) -> SqlitePool {
            let opts = SqliteConnectOptions::from_str(url)
                .unwrap()
                .journal_mode(SqliteJournalMode::Wal)
                .busy_timeout(Duration::from_millis(0));
            SqlitePoolOptions::new()
                .max_connections(1)
                .connect_with(opts)
                .await
                .unwrap()
        }

        let holder = connect_single(&url).await;
        sqlx::query("CREATE TABLE t (id INTEGER PRIMARY KEY)")
            .execute(&holder)
            .await
            .unwrap();

        // Hold the write lock open via an uncommitted IMMEDIATE transaction.
        let mut tx = holder
            .begin()
            .await
            .unwrap();
        sqlx::query("INSERT INTO t DEFAULT VALUES")
            .execute(&mut *tx)
            .await
            .unwrap();

        let writer = connect_single(&url).await;
        let attempts = AtomicUsize::new(0);
        let result: Result<sqlx::sqlite::SqliteQueryResult, sqlx::Error> =
            retry_on_busy(3, 1, || {
                attempts.fetch_add(1, Ordering::SeqCst);
                let writer = &writer;
                async move {
                    sqlx::query("INSERT INTO t DEFAULT VALUES")
                        .execute(writer)
                        .await
                }
            })
            .await;

        assert!(result.is_err());
        assert!(is_sqlite_busy_or_locked(&result.unwrap_err()));
        assert_eq!(attempts.load(Ordering::SeqCst), 3);

        // Releasing the lock lets a fresh attempt succeed.
        tx.rollback()
            .await
            .unwrap();
        sqlx::query("INSERT INTO t DEFAULT VALUES")
            .execute(&writer)
            .await
            .expect("insert should succeed once the lock is released");
    }
}
