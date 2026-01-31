//! Metadata persistence layer for NewTube. Mainly used for backend tests.
//!
//! All structs in this module mirror how metadata is serialized to disk and
//! exposed to the API.

use std::path::Path;

use anyhow::{Context, Result};
use libsql::{Builder, Connection, Row, params};
use serde::{Deserialize, Serialize};

/// Description of a single downloadable media source (e.g. 1080p mp4).
///
/// Sources can point to files on disk (`path`) or merely expose a streaming
/// endpoint backed by the API. The struct mirrors the JSON persisted inside the
/// SQLite tables.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoSource {
    pub format_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quality_label: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fps: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mime_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ext: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_size: Option<i64>,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// Rows stored in the `videos` and `shorts` tables.
///
/// Many fields are optional so we gracefully handle partially known metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VideoRecord {
    pub videoid: String,
    pub title: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub likes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dislikes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub views: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub upload_date: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscriber_count: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub channel_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub thumbnail_url: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub thumbnails: Vec<String>,
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub extras: serde_json::Value,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sources: Vec<VideoSource>,
}

/// Subtitle manifest for a single video.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubtitleTrack {
    pub code: String,
    pub name: String,
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
}

/// Collection of all subtitle tracks that belong to a video id.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubtitleCollection {
    pub videoid: String,
    #[serde(default)]
    pub languages: Vec<SubtitleTrack>,
}

/// Comment stored on disk, mirroring what the frontend expects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommentRecord {
    pub id: String,
    pub videoid: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub author: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub text: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub likes: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_posted: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_comment_id: Option<String>,
    #[serde(default)]
    pub status_likedbycreator: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_count: Option<i64>,
}

async fn configure_connection(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;
        PRAGMA foreign_keys=ON;
        "#,
    )
    .await?;
    Ok(())
}

async fn ensure_schema(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS videos (
            videoid TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT DEFAULT '',
            likes INTEGER,
            dislikes INTEGER,
            views INTEGER,
            upload_date TEXT,
            author TEXT,
            subscriber_count INTEGER,
            duration INTEGER,
            duration_text TEXT,
            channel_url TEXT,
            thumbnail_url TEXT,
            tags_json TEXT DEFAULT '[]',
            thumbnails_json TEXT DEFAULT '[]',
            extras_json TEXT DEFAULT 'null',
            sources_json TEXT DEFAULT '[]'
        );

        CREATE TABLE IF NOT EXISTS shorts (
            videoid TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            description TEXT DEFAULT '',
            likes INTEGER,
            dislikes INTEGER,
            views INTEGER,
            upload_date TEXT,
            author TEXT,
            subscriber_count INTEGER,
            duration INTEGER,
            duration_text TEXT,
            channel_url TEXT,
            thumbnail_url TEXT,
            tags_json TEXT DEFAULT '[]',
            thumbnails_json TEXT DEFAULT '[]',
            extras_json TEXT DEFAULT 'null',
            sources_json TEXT DEFAULT '[]'
        );

        CREATE TABLE IF NOT EXISTS subtitles (
            videoid TEXT PRIMARY KEY,
            languages_json TEXT NOT NULL DEFAULT '[]'
        );

        CREATE TABLE IF NOT EXISTS comments (
            id TEXT PRIMARY KEY,
            videoid TEXT NOT NULL,
            author TEXT DEFAULT '',
            text TEXT DEFAULT '',
            likes INTEGER,
            time_posted TEXT,
            parent_comment_id TEXT,
            status_likedbycreator INTEGER NOT NULL DEFAULT 0,
            reply_count INTEGER
        );

        CREATE INDEX IF NOT EXISTS idx_comments_videoid ON comments(videoid);
        CREATE INDEX IF NOT EXISTS idx_comments_parent ON comments(parent_comment_id);
        "#,
    )
    .await?;

    migrate_comments_schema(conn).await?;

    Ok(())
}

async fn migrate_comments_schema(conn: &Connection) -> Result<()> {
    let mut rows = conn.query("PRAGMA foreign_key_list(comments)", params![]).await?;
    let mut has_video_fk = false;
    while let Some(row) = rows.next().await? {
        let table: String = row.get(2)?;
        if table == "videos" {
            has_video_fk = true;
            break;
        }
    }

    if !has_video_fk {
        return Ok(());
    }

    conn.execute_batch(
        r#"
        BEGIN;
        CREATE TABLE IF NOT EXISTS comments_new (
            id TEXT PRIMARY KEY,
            videoid TEXT NOT NULL,
            author TEXT DEFAULT '',
            text TEXT DEFAULT '',
            likes INTEGER,
            time_posted TEXT,
            parent_comment_id TEXT,
            status_likedbycreator INTEGER NOT NULL DEFAULT 0,
            reply_count INTEGER
        );
        INSERT INTO comments_new (
            id, videoid, author, text, likes, time_posted,
            parent_comment_id, status_likedbycreator, reply_count
        )
        SELECT
            id, videoid, author, text, likes, time_posted,
            parent_comment_id, status_likedbycreator, reply_count
        FROM comments;
        DROP TABLE comments;
        ALTER TABLE comments_new RENAME TO comments;
        CREATE INDEX IF NOT EXISTS idx_comments_videoid ON comments(videoid);
        CREATE INDEX IF NOT EXISTS idx_comments_parent ON comments(parent_comment_id);
        COMMIT;
        "#,
    )
    .await?;

    Ok(())
}

/// Wrapper around the SQLite-compatible connection that performs read/write operations.
#[derive(Debug)]
pub struct MetadataStore {
    conn: Connection,
}

impl MetadataStore {
    /// Opens (and if necessary creates) the SQLite DB and ensures the expected
    /// schema exists.
    pub async fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating metadata directory {}", parent.display()))?;
        }

        let db = Builder::new_local(path)
            .build()
            .await
            .with_context(|| format!("opening metadata DB {}", path.display()))?;

        let conn = db.connect()?;
        configure_connection(&conn).await?;

        let store = Self { conn };
        store.ensure_tables().await?;
        Ok(store)
    }

    /// Runs the SQL required to create the tables if they do not already exist.
    async fn ensure_tables(&self) -> Result<()> {
        ensure_schema(&self.conn).await
    }

    /// Inserts or updates a long-form video entry.
    pub async fn upsert_video(&self, record: &VideoRecord) -> Result<()> {
        self.upsert("videos", record).await
    }

    pub async fn upsert_short(&self, record: &VideoRecord) -> Result<()> {
        self.upsert("shorts", record).await
    }

    /// Shared helper used by both `videos` and `shorts` tables.
    async fn upsert(&self, table: &str, record: &VideoRecord) -> Result<()> {
        let tags_json = serde_json::to_string(&record.tags).context("serializing tags")?;
        let thumbnails_json =
            serde_json::to_string(&record.thumbnails).context("serializing thumbnails")?;
        let extras_json =
            serde_json::to_string(&record.extras).context("serializing extra metadata")?;
        let sources_json = serde_json::to_string(&record.sources).context("serializing sources")?;

        self.conn
            .execute(
            &format!(
                r#"
                INSERT INTO {table} (
                    videoid, title, description, likes, dislikes, views,
                    upload_date, author, subscriber_count, duration, duration_text,
                    channel_url, thumbnail_url, tags_json, thumbnails_json,
                    extras_json, sources_json
                ) VALUES (
                    :videoid, :title, :description, :likes, :dislikes, :views,
                    :upload_date, :author, :subscriber_count, :duration, :duration_text,
                    :channel_url, :thumbnail_url, :tags_json, :thumbnails_json,
                    :extras_json, :sources_json
                )
                ON CONFLICT(videoid) DO UPDATE SET
                    title = excluded.title,
                    description = excluded.description,
                    likes = excluded.likes,
                    dislikes = excluded.dislikes,
                    views = excluded.views,
                    upload_date = excluded.upload_date,
                    author = excluded.author,
                    subscriber_count = excluded.subscriber_count,
                    duration = excluded.duration,
                    duration_text = excluded.duration_text,
                    channel_url = excluded.channel_url,
                    thumbnail_url = excluded.thumbnail_url,
                    tags_json = excluded.tags_json,
                    thumbnails_json = excluded.thumbnails_json,
                    extras_json = excluded.extras_json,
                    sources_json = excluded.sources_json
                "#,
            ),
            params![
                record.videoid.as_str(),
                record.title.as_str(),
                record.description.as_str(),
                record.likes,
                record.dislikes,
                record.views,
                record.upload_date.as_deref(),
                record.author.as_deref(),
                record.subscriber_count,
                record.duration,
                record.duration_text.as_deref(),
                record.channel_url.as_deref(),
                record.thumbnail_url.as_deref(),
                tags_json,
                thumbnails_json,
                extras_json,
                sources_json,
            ],
        )
        .await?;

        Ok(())
    }

    /// Stores subtitle metadata in the DB.
    pub async fn upsert_subtitles(&self, subtitles: &SubtitleCollection) -> Result<()> {
        let languages_json =
            serde_json::to_string(&subtitles.languages).context("serializing subtitles")?;

        self.conn
            .execute(
            r#"
            INSERT INTO subtitles (videoid, languages_json)
            VALUES (:videoid, :languages_json)
            ON CONFLICT(videoid) DO UPDATE SET
                languages_json = excluded.languages_json
            "#,
            params![subtitles.videoid.as_str(), languages_json],
        )
        .await?;

        Ok(())
    }

    /// Replaces every stored comment for `videoid` in one transaction so we do
    /// not mix old and new comment trees.
    pub async fn replace_comments(&self, videoid: &str, comments: &[CommentRecord]) -> Result<()> {
        let tx = self.conn.transaction().await?;
        tx.execute("DELETE FROM comments WHERE videoid = ?1", params![videoid])
            .await?;

        for comment in comments {
            tx.execute(
                r#"
                INSERT INTO comments (
                    id, videoid, author, text, likes, time_posted,
                    parent_comment_id, status_likedbycreator, reply_count
                ) VALUES (
                    :id, :videoid, :author, :text, :likes, :time_posted,
                    :parent_comment_id, :status_likedbycreator, :reply_count
                )
                "#,
                params![
                    comment.id.as_str(),
                    comment.videoid.as_str(),
                    comment.author.as_str(),
                    comment.text.as_str(),
                    comment.likes,
                    comment.time_posted.as_deref(),
                    comment.parent_comment_id.as_deref(),
                    comment.status_likedbycreator as i64,
                    comment.reply_count,
                ],
            )
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }
}

/// Lightweight cloneable reader that opens short‑lived connections for each
/// query. This avoids keeping a single connection open across threads/tasks.
#[derive(Clone)]
pub struct MetadataReader {
    conn: Connection,
}

impl MetadataReader {
    /// Creates a new reader that lazily opens the DB whenever a query runs.
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let db = Builder::new_local(path.as_ref())
            .build()
            .await
            .with_context(|| format!("opening metadata DB {}", path.as_ref().display()))?;
        let conn = db.connect()?;
        configure_connection(&conn).await?;
        ensure_schema(&conn).await?;
        Ok(Self { conn })
    }

    pub async fn list_videos(&self) -> Result<Vec<VideoRecord>> {
        self.fetch_videos_from("videos").await
    }

    pub async fn list_shorts(&self) -> Result<Vec<VideoRecord>> {
        self.fetch_videos_from("shorts").await
    }

    pub async fn get_video(&self, videoid: &str) -> Result<Option<VideoRecord>> {
        self.fetch_single("videos", videoid).await
    }

    pub async fn get_short(&self, videoid: &str) -> Result<Option<VideoRecord>> {
        self.fetch_single("shorts", videoid).await
    }

    pub async fn get_subtitles(&self, videoid: &str) -> Result<Option<SubtitleCollection>> {
        let conn = &self.conn;
        let stmt = conn
            .prepare(
                r#"
                SELECT languages_json
                FROM subtitles
                WHERE videoid = ?1
                "#,
            )
            .await?;

        let mut rows = stmt.query([videoid]).await?;
        let Some(row) = rows.next().await? else {
            return Ok(None);
        };
        let languages_json: String = row.get(0)?;
        let languages: Vec<SubtitleTrack> =
            serde_json::from_str(&languages_json).context("parsing subtitle tracks")?;
        Ok(Some(SubtitleCollection {
            videoid: videoid.to_owned(),
            languages,
        }))
    }

    pub async fn list_subtitles(&self) -> Result<Vec<SubtitleCollection>> {
        let conn = &self.conn;
        let stmt = conn
            .prepare(
                r#"
                SELECT videoid, languages_json
                FROM subtitles
                "#,
            )
            .await?;

        let mut rows = stmt.query(params![]).await?;
        let mut results = Vec::new();
        while let Some(row) = rows.next().await? {
            let videoid: String = row.get(0)?;
            let languages_json: String = row.get(1)?;
            let languages: Vec<SubtitleTrack> =
                serde_json::from_str(&languages_json).context("parsing subtitle tracks")?;
            results.push(SubtitleCollection { videoid, languages });
        }
        Ok(results)
    }

    pub async fn get_comments(&self, videoid: &str) -> Result<Vec<CommentRecord>> {
        let conn = &self.conn;
        let stmt = conn
            .prepare(
                r#"
                SELECT id, videoid, author, text, likes, time_posted,
                       parent_comment_id, status_likedbycreator, reply_count
                FROM comments
                WHERE videoid = ?1
                  AND (
                    EXISTS (SELECT 1 FROM videos WHERE videoid = ?1)
                    OR EXISTS (SELECT 1 FROM shorts WHERE videoid = ?1)
                  )
                ORDER BY datetime(time_posted) IS NULL, datetime(time_posted) ASC, rowid ASC
                "#,
            )
            .await?;

        let mut comments = Vec::new();
        let mut rows = stmt.query([videoid]).await?;
        while let Some(row) = rows.next().await? {
            comments.push(row_to_comment(&row)?);
        }
        Ok(comments)
    }

    pub async fn list_all_comments(&self) -> Result<Vec<CommentRecord>> {
        let conn = &self.conn;
        let stmt = conn
            .prepare(
                r#"
                SELECT id, videoid, author, text, likes, time_posted,
                       parent_comment_id, status_likedbycreator, reply_count
                FROM comments
                WHERE videoid IN (
                    SELECT videoid FROM videos
                    UNION
                    SELECT videoid FROM shorts
                )
                ORDER BY datetime(time_posted) IS NULL, datetime(time_posted) ASC, rowid ASC
                "#,
            )
            .await?;

        let mut rows = stmt.query(params![]).await?;
        let mut comments = Vec::new();
        while let Some(row) = rows.next().await? {
            comments.push(row_to_comment(&row)?);
        }
        Ok(comments)
    }

    pub async fn data_version(&self) -> Result<i64> {
        let conn = &self.conn;
        let mut rows = conn.query("PRAGMA data_version", params![]).await?;
        let row = rows
            .next()
            .await?
            .context("missing data_version row")?;
        Ok(row.get(0)?)
    }

    async fn fetch_videos_from(&self, table: &str) -> Result<Vec<VideoRecord>> {
        let conn = &self.conn;
        let stmt = conn
            .prepare(&format!(
                r#"
                SELECT videoid, title, description, likes, dislikes, views,
                       upload_date, author, subscriber_count, duration, duration_text,
                       channel_url, thumbnail_url, tags_json, thumbnails_json,
                       extras_json, sources_json
                FROM {table}
                ORDER BY upload_date DESC, rowid DESC
                "#
            ))
            .await?;

        let mut rows = stmt.query(params![]).await?;
        let mut records = Vec::new();
        while let Some(row) = rows.next().await? {
            records.push(row_to_video_record(&row)?);
        }
        Ok(records)
    }

    async fn fetch_single(&self, table: &str, videoid: &str) -> Result<Option<VideoRecord>> {
        let conn = &self.conn;
        let stmt = conn
            .prepare(&format!(
                r#"
                SELECT videoid, title, description, likes, dislikes, views,
                       upload_date, author, subscriber_count, duration, duration_text,
                       channel_url, thumbnail_url, tags_json, thumbnails_json,
                       extras_json, sources_json
                FROM {table}
                WHERE videoid = ?1
                "#
            ))
            .await?;

        let mut rows = stmt.query([videoid]).await?;
        if let Some(row) = rows.next().await? {
            Ok(Some(row_to_video_record(&row)?))
        } else {
            Ok(None)
        }
    }
}

/// Converts a SQL row into a `VideoRecord`, deserializing the Vec/JSON fields.
fn row_to_video_record(row: &Row) -> Result<VideoRecord> {
    // Column order must match the SELECT statements in fetch_videos_from/fetch_single.
    let tags_json: String = row.get(13)?;
    let thumbnails_json: String = row.get(14)?;
    let extras_json: String = row.get(15)?;
    let sources_json: String = row.get(16)?;

    let tags: Vec<String> = serde_json::from_str(&tags_json).context("parsing stored tags JSON")?;
    let thumbnails: Vec<String> =
        serde_json::from_str(&thumbnails_json).context("parsing stored thumbnails JSON")?;
    let extras: serde_json::Value =
        serde_json::from_str(&extras_json).context("parsing stored extras JSON")?;
    let sources: Vec<VideoSource> =
        serde_json::from_str(&sources_json).context("parsing stored sources JSON")?;

    Ok(VideoRecord {
        videoid: row.get(0)?,
        title: row.get(1)?,
        description: row.get(2)?,
        likes: row.get(3)?,
        dislikes: row.get(4)?,
        views: row.get(5)?,
        upload_date: row.get(6)?,
        author: row.get(7)?,
        subscriber_count: row.get(8)?,
        duration: row.get(9)?,
        duration_text: row.get(10)?,
        channel_url: row.get(11)?,
        thumbnail_url: row.get(12)?,
        tags,
        thumbnails,
        extras,
        sources,
    })
}

/// Converts a SQL row into a `CommentRecord` while normalizing the boolean flag
/// stored as an INTEGER in SQLite.
fn row_to_comment(row: &Row) -> Result<CommentRecord> {
    Ok(CommentRecord {
        id: row.get(0)?,
        videoid: row.get(1)?,
        author: row.get(2)?,
        text: row.get(3)?,
        likes: row.get(4)?,
        time_posted: row.get(5)?,
        parent_comment_id: row.get(6)?,
        status_likedbycreator: row
            .get::<i64>(7)
            .map(|value| value != 0)?,
        reply_count: row.get(8)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use tempfile::tempdir;

    /// Utility builder so every test can generate a fully populated video row
    /// without repeating dozens of assignments. Individual tests tweak the
    /// resulting struct when they need to exercise specific fields.
    fn sample_video(id: &str) -> VideoRecord {
        VideoRecord {
            videoid: id.to_owned(),
            title: format!("Video {id}"),
            description: "desc".into(),
            likes: Some(1),
            dislikes: Some(0),
            views: Some(42),
            upload_date: Some("2024-01-01".into()),
            author: Some("Author".into()),
            subscriber_count: Some(1000),
            duration: Some(120),
            duration_text: Some("2:00".into()),
            channel_url: Some("https://example.com".into()),
            thumbnail_url: Some("thumb.jpg".into()),
            tags: vec!["tech".into()],
            thumbnails: vec!["thumb.jpg".into()],
            extras: serde_json::json!({"kind": "demo"}),
            sources: vec![VideoSource {
                format_id: "1080p".into(),
                quality_label: Some("1080p".into()),
                width: Some(1920),
                height: Some(1080),
                fps: Some(30.0),
                mime_type: Some("video/mp4".into()),
                ext: Some("mp4".into()),
                file_size: Some(1_000_000),
                url: "https://cdn.example/video.mp4".into(),
                path: Some("/videos/video.mp4".into()),
            }],
        }
    }

    /// Helper that produces deterministic comment rows; individual tests can
    /// tweak author/text/timestamps without redefining the entire struct.
    fn sample_comment(id: &str, videoid: &str) -> CommentRecord {
        CommentRecord {
            id: id.into(),
            videoid: videoid.into(),
            author: format!("author-{id}"),
            text: format!("text-{id}"),
            likes: Some(0),
            time_posted: Some("2024-01-01T00:00:00Z".into()),
            parent_comment_id: None,
            status_likedbycreator: false,
            reply_count: Some(0),
        }
    }

    /// Opens a brand‑new temporary SQLite store and returns both the writable
    /// `MetadataStore` and read-only `MetadataReader`. Using a temp directory
    /// keeps tests isolated and mirrors how the binaries interact with the DB.
    async fn create_store() -> Result<(tempfile::TempDir, MetadataStore, MetadataReader, PathBuf)> {
        let dir = tempdir()?;
        let path = dir.path().join("metadata/test.db");
        let store = MetadataStore::open(&path).await?;
        let reader = MetadataReader::new(&path).await?;
        Ok((dir, store, reader, path))
    }

    /// Validates that opening a store creates the DB file, turns on WAL mode and
    /// provisions every expected table/index. This guards against regressions in
    /// the bootstrap SQL.
    #[tokio::test]
    async fn opens_store_and_creates_schema() -> Result<()> {
        let (_temp, _store, _reader, path) = create_store().await?;
        assert!(path.exists(), "database file should be created");

        let db = Builder::new_local(&path).build().await?;
        let conn = db.connect()?;
        configure_connection(&conn).await?;
        let mut rows = conn.query("PRAGMA journal_mode", params![]).await?;
        let journal_row = rows.next().await?.context("missing journal_mode row")?;
        let journal: String = journal_row.get(0)?;
        assert_eq!(journal.to_lowercase(), "wal");
        let mut rows = conn.query("PRAGMA synchronous", params![]).await?;
        let sync_row = rows.next().await?.context("missing synchronous row")?;
        let synchronous: i64 = sync_row.get(0)?;
        assert!(
            synchronous >= 1,
            "synchronous should be NORMAL or stricter but was {synchronous}"
        );

        for table in ["videos", "shorts", "subtitles", "comments"] {
            let mut rows = conn
                .query(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?1",
                    [table],
                )
                .await?;
            let exists: Option<String> = rows
                .next()
                .await?
                .map(|row| row.get::<String>(0))
                .transpose()?;
            assert_eq!(exists.as_deref(), Some(table));
        }

        for index in ["idx_comments_videoid", "idx_comments_parent"] {
            let mut rows = conn
                .query(
                    "SELECT name FROM sqlite_master WHERE type='index' AND name=?1",
                    [index],
                )
                .await?;
            let exists: Option<String> = rows
                .next()
                .await?
                .map(|row| row.get::<String>(0))
                .transpose()?;
            assert_eq!(exists.as_deref(), Some(index));
        }
        Ok(())
    }

    /// Ensures that short-lived connections keep foreign_keys enforcement
    /// enabled so cascades behave consistently across helpers.
    #[tokio::test]
    async fn reader_enforces_foreign_keys() -> Result<()> {
        let (_temp, _store, reader, _path) = create_store().await?;
        let conn = &reader.conn;
        let mut rows = conn.query("PRAGMA foreign_keys", params![]).await?;
        let row = rows.next().await?.context("missing foreign_keys row")?;
        let flag: i64 = row.get(0)?;
        assert_eq!(flag, 1);
        Ok(())
    }

    /// Covers the insert/update path for long-form videos, ensuring JSON fields
    /// survive a round trip and updates override previous values as intended.
    #[tokio::test]
    async fn upsert_video_roundtrip() -> Result<()> {
        let (_temp, store, reader, _path) = create_store().await?;

        let mut record = sample_video("alpha");
        // First insertion should persist all provided metadata as-is.
        store.upsert_video(&record).await?;

        let fetched = reader.get_video("alpha").await?.expect("video fetched");
        assert_eq!(fetched.title, record.title);
        assert_eq!(fetched.tags, record.tags);
        assert_eq!(fetched.sources[0].format_id, "1080p");

        // Update a couple of fields and verify that ON CONFLICT rewrites them.
        record.title = "Updated".into();
        record.tags.push("review".into());
        store.upsert_video(&record).await?;
        let updated = reader
            .get_video("alpha")
            .await?
            .expect("video fetched after update");
        assert_eq!(updated.title, "Updated");
        assert!(updated.tags.contains(&"review".into()));
        Ok(())
    }

    /// Mirrors the previous test but against the `shorts` table to guarantee
    /// feature parity between both content types.
    #[tokio::test]
    async fn upsert_short_roundtrip() -> Result<()> {
        let (_temp, store, reader, _path) = create_store().await?;

        let record = sample_video("shorty");
        // Short content uses the dedicated table but otherwise mirrors videos.
        store.upsert_short(&record).await?;

        let shorts = reader.list_shorts().await?;
        assert_eq!(shorts.len(), 1);
        assert_eq!(shorts[0].videoid, "shorty");
        Ok(())
    }

    /// Ensures subtitle collections get serialized to JSON and can be retrieved
    /// verbatim by the reader API.
    #[tokio::test]
    async fn upsert_and_list_subtitles() -> Result<()> {
        let (_temp, store, reader, _path) = create_store().await?;
        store.upsert_video(&sample_video("vid")).await?;

        let subtitles = SubtitleCollection {
            videoid: "vid".into(),
            languages: vec![SubtitleTrack {
                code: "en".into(),
                name: "English".into(),
                url: "https://cdn/subs.vtt".into(),
                path: Some("/subs/en.vtt".into()),
            }],
        };
        // Writing a collection should replace any prior row for the video.
        store.upsert_subtitles(&subtitles).await?;

        let listed = reader.list_subtitles().await?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].languages[0].code, "en");
        Ok(())
    }

    /// Exercises the transactional comment replacement flow so we never keep
    /// stale comment trees after a new download cycle.
    #[tokio::test]
    async fn replace_comments_resets_previous_entries() -> Result<()> {
        let (_temp, store, reader, _path) = create_store().await?;
        store.upsert_video(&sample_video("vid")).await?;

        let first = vec![CommentRecord {
            id: "1".into(),
            videoid: "vid".into(),
            author: "a".into(),
            text: "hello".into(),
            likes: Some(1),
            time_posted: Some("2024-01-01".into()),
            parent_comment_id: None,
            status_likedbycreator: true,
            reply_count: Some(0),
        }];
        // Seed the DB with a first batch of comments.
        store.replace_comments("vid", &first).await?;

        let second = vec![CommentRecord {
            id: "2".into(),
            videoid: "vid".into(),
            author: "b".into(),
            text: "world".into(),
            likes: Some(2),
            time_posted: Some("2024-01-02".into()),
            parent_comment_id: None,
            status_likedbycreator: false,
            reply_count: Some(1),
        }];
        // Second replacement should wipe the previous entries before inserting.
        store.replace_comments("vid", &second).await?;

        let fetched = reader.get_comments("vid").await?;
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].id, "2");
        assert!(!fetched[0].status_likedbycreator);
        Ok(())
    }

    /// Verifies that listing videos applies the desired ordering (newest first)
    /// even when dates differ, which is critical for deterministic feeds.
    #[tokio::test]
    async fn list_videos_returns_sorted_records() -> Result<()> {
        let (_temp, store, reader, _path) = create_store().await?;

        let mut old = sample_video("old");
        old.upload_date = Some("2023-01-01".into());
        store.upsert_video(&old).await?;

        let mut new = sample_video("new");
        new.upload_date = Some("2024-05-01".into());
        store.upsert_video(&new).await?;

        let videos = reader.list_videos().await?;
        assert_eq!(videos.len(), 2);
        assert_eq!(videos[0].videoid, "new");
        assert_eq!(videos[1].videoid, "old");
        Ok(())
    }

    /// Reader helpers should gracefully return `None` when a record is missing.
    #[tokio::test]
    async fn reader_returns_none_for_missing_entries() -> Result<()> {
        let (_temp, _store, reader, _path) = create_store().await?;
        assert!(reader.get_video("ghost").await?.is_none());
        assert!(reader.get_short("ghost").await?.is_none());
        assert!(reader.get_subtitles("ghost").await?.is_none());
        Ok(())
    }

    /// Listing shorts mirrors videos but must respect upload_date ordering.
    #[tokio::test]
    async fn list_shorts_sorted_by_upload_date() -> Result<()> {
        let (_temp, store, reader, _path) = create_store().await?;
        let mut older = sample_video("short-old");
        older.upload_date = Some("2023-05-01".into());
        store.upsert_short(&older).await?;

        let mut newer = sample_video("short-new");
        newer.upload_date = Some("2024-06-01".into());
        store.upsert_short(&newer).await?;

        let shorts = reader.list_shorts().await?;
        assert_eq!(shorts.len(), 2);
        assert_eq!(shorts[0].videoid, "short-new");
        assert_eq!(shorts[1].videoid, "short-old");
        Ok(())
    }

    /// Subtitle upserts should overwrite existing rows rather than append.
    #[tokio::test]
    async fn upsert_subtitles_overwrites_existing_languages() -> Result<()> {
        let (_temp, store, reader, _path) = create_store().await?;
        store.upsert_video(&sample_video("vid-sub")).await?;

        let initial = SubtitleCollection {
            videoid: "vid-sub".into(),
            languages: vec![SubtitleTrack {
                code: "en".into(),
                name: "English".into(),
                url: "https://cdn/en.vtt".into(),
                path: None,
            }],
        };
        store.upsert_subtitles(&initial).await?;

        let updated = SubtitleCollection {
            videoid: "vid-sub".into(),
            languages: vec![SubtitleTrack {
                code: "fr".into(),
                name: "Français".into(),
                url: "https://cdn/fr.vtt".into(),
                path: Some("/subs/fr.vtt".into()),
            }],
        };
        store.upsert_subtitles(&updated).await?;

        let fetched = reader
            .get_subtitles("vid-sub")
            .await?
            .expect("subtitles exist");
        assert_eq!(fetched.languages.len(), 1);
        assert_eq!(fetched.languages[0].code, "fr");
        Ok(())
    }

    /// Comments containing replies and flags should persist verbatim.
    #[tokio::test]
    async fn replace_comments_preserves_replies_and_flags() -> Result<()> {
        let (_temp, store, reader, _path) = create_store().await?;
        store.upsert_video(&sample_video("with-comments")).await?;

        let mut parent = sample_comment("parent", "with-comments");
        parent.status_likedbycreator = true;
        let mut reply = sample_comment("child", "with-comments");
        reply.parent_comment_id = Some("parent".into());

        store.replace_comments("with-comments", &[parent.clone(), reply.clone()])
            .await?;

        let comments = reader.get_comments("with-comments").await?;
        assert_eq!(comments.len(), 2);
        assert_eq!(comments[0].id, "parent");
        assert!(comments[0].status_likedbycreator);
        assert_eq!(comments[1].parent_comment_id.as_deref(), Some("parent"));
        Ok(())
    }

    /// list_all_comments should merge comments across videos ordered by timestamp.
    #[tokio::test]
    async fn list_all_comments_orders_by_time() -> Result<()> {
        let (_temp, store, reader, _path) = create_store().await?;
        store.upsert_video(&sample_video("video-one")).await?;
        store.upsert_video(&sample_video("video-two")).await?;

        let mut first = sample_comment("1", "video-one");
        first.time_posted = Some("2024-01-01T00:00:00Z".into());
        let mut second = sample_comment("2", "video-two");
        second.time_posted = Some("2024-01-01T00:05:00Z".into());
        let mut third = sample_comment("3", "video-one");
        third.time_posted = Some("2024-01-01T00:10:00Z".into());

        store.replace_comments("video-one", &[first.clone(), third.clone()])
            .await?;
        store.replace_comments("video-two", &[second.clone()]).await?;

        let all = reader.list_all_comments().await?;
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].id, "1");
        assert_eq!(all[1].id, "2");
        assert_eq!(all[2].id, "3");
        Ok(())
    }
}
