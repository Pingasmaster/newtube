//! Metadata persistence layer for NewTube. Mainly used for backend tests.
//!
//! All structs in this module mirror how metadata is serialized to disk and
//! exposed to the API.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use libsql::{Connection, Database, params};
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

/// Wrapper around the SQLite-compatible connection that performs read/write operations.
#[derive(Debug)]
pub struct MetadataStore {
    db: Database,
}

impl MetadataStore {
    /// Opens (and if necessary creates) the SQLite DB and ensures the expected
    /// schema exists. WAL mode is enabled to avoid readers blocking writers.
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating metadata directory {}", parent.display()))?;
        }

        let db = Database::open_local(path)
            .with_context(|| format!("opening metadata DB {}", path.display()))?;

        let store = Self { db };
        store.ensure_tables()?;
        Ok(store)
    }

    /// Runs the SQL required to create the tables if they do not already exist.
    fn ensure_tables(&self) -> Result<()> {
        let conn = self.db.connect()?;
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
                reply_count INTEGER,
                FOREIGN KEY (videoid) REFERENCES videos(videoid) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_comments_videoid ON comments(videoid);
            CREATE INDEX IF NOT EXISTS idx_comments_parent ON comments(parent_comment_id);
            "#,
        )?;
        Ok(())
    }

    /// Inserts or updates a long-form video entry.
    pub fn upsert_video(&self, record: &VideoRecord) -> Result<()> {
        self.upsert("videos", record)
    }

    pub fn upsert_short(&self, record: &VideoRecord) -> Result<()> {
        self.upsert("shorts", record)
    }

    /// Shared helper used by both `videos` and `shorts` tables.
    fn upsert(&self, table: &str, record: &VideoRecord) -> Result<()> {
        let tags_json = serde_json::to_string(&record.tags).context("serializing tags")?;
        let thumbnails_json =
            serde_json::to_string(&record.thumbnails).context("serializing thumbnails")?;
        let extras_json =
            serde_json::to_string(&record.extras).context("serializing extra metadata")?;
        let sources_json = serde_json::to_string(&record.sources).context("serializing sources")?;

        self.conn.execute(
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
                record.videoid,
                record.title,
                record.description,
                record.likes,
                record.dislikes,
                record.views,
                record.upload_date,
                record.author,
                record.subscriber_count,
                record.duration,
                record.duration_text,
                record.channel_url,
                record.thumbnail_url,
                tags_json,
                thumbnails_json,
                extras_json,
                sources_json,
            ],
        )?;

        Ok(())
    }

    /// Stores subtitle metadata in the DB.
    pub fn upsert_subtitles(&self, subtitles: &SubtitleCollection) -> Result<()> {
        let languages_json =
            serde_json::to_string(&subtitles.languages).context("serializing subtitles")?;

        self.conn.execute(
            r#"
            INSERT INTO subtitles (videoid, languages_json)
            VALUES (:videoid, :languages_json)
            ON CONFLICT(videoid) DO UPDATE SET
                languages_json = excluded.languages_json
            "#,
            params![subtitles.videoid, languages_json],
        )?;

        Ok(())
    }

    /// Replaces every stored comment for `videoid` in one transaction so we do
    /// not mix old and new comment trees.
    pub fn replace_comments(&mut self, videoid: &str, comments: &[CommentRecord]) -> Result<()> {
        let tx = self.conn.transaction()?;
        tx.execute("DELETE FROM comments WHERE videoid = ?1", params![videoid])?;

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
                    comment.id,
                    comment.videoid,
                    comment.author,
                    comment.text,
                    comment.likes,
                    comment.time_posted,
                    comment.parent_comment_id,
                    comment.status_likedbycreator as i64,
                    comment.reply_count,
                ],
            )?;
        }

        tx.commit()?;
        Ok(())
    }
}

/// Lightweight cloneable reader that opens short‑lived connections for each
/// query. This avoids keeping a single connection open across threads/tasks.
#[derive(Clone)]
pub struct MetadataReader {
    db_path: PathBuf,
}

impl MetadataReader {
    /// Creates a new reader that lazily opens the DB whenever a query runs.
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            db_path: path.as_ref().to_path_buf(),
        })
    }

    fn with_connection<F, T>(&self, f: F) -> Result<T>
    where
        F: FnOnce(&Connection) -> Result<T>,
    {
        // Open a dedicated connection per invocation so long running queries
        // do not block unrelated threads.
        let conn = Connection::open(&self.db_path)
            .with_context(|| format!("opening metadata DB {}", self.db_path.display()))?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        f(&conn)
    }

    pub fn list_videos(&self) -> Result<Vec<VideoRecord>> {
        self.fetch_videos_from("videos")
    }

    pub fn list_shorts(&self) -> Result<Vec<VideoRecord>> {
        self.fetch_videos_from("shorts")
    }

    pub fn get_video(&self, videoid: &str) -> Result<Option<VideoRecord>> {
        self.fetch_single("videos", videoid)
    }

    pub fn get_short(&self, videoid: &str) -> Result<Option<VideoRecord>> {
        self.fetch_single("shorts", videoid)
    }

    pub fn get_subtitles(&self, videoid: &str) -> Result<Option<SubtitleCollection>> {
        self.with_connection(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT languages_json
                FROM subtitles
                WHERE videoid = ?1
                "#,
            )?;

            let json: Option<String> = stmt.query_row([videoid], |row| row.get(0)).optional()?;

            if let Some(languages_json) = json {
                let languages: Vec<SubtitleTrack> =
                    serde_json::from_str(&languages_json).context("parsing subtitle tracks")?;
                Ok(Some(SubtitleCollection {
                    videoid: videoid.to_owned(),
                    languages,
                }))
            } else {
                Ok(None)
            }
        })
    }

    pub fn list_subtitles(&self) -> Result<Vec<SubtitleCollection>> {
        self.with_connection(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT videoid, languages_json
                FROM subtitles
                "#,
            )?;

            let mut rows = stmt.query([])?;
            let mut results = Vec::new();
            while let Some(row) = rows.next()? {
                let videoid: String = row.get(0)?;
                let languages_json: String = row.get(1)?;
                let languages: Vec<SubtitleTrack> =
                    serde_json::from_str(&languages_json).context("parsing subtitle tracks")?;
                results.push(SubtitleCollection { videoid, languages });
            }
            Ok(results)
        })
    }

    pub fn get_comments(&self, videoid: &str) -> Result<Vec<CommentRecord>> {
        self.with_connection(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT id, videoid, author, text, likes, time_posted,
                       parent_comment_id, status_likedbycreator, reply_count
                FROM comments
                WHERE videoid = ?1
                ORDER BY time_posted ASC
                "#,
            )?;

            let mut comments = Vec::new();
            let mut rows = stmt.query([videoid])?;
            while let Some(row) = rows.next()? {
                comments.push(row_to_comment(row)?);
            }
            Ok(comments)
        })
    }

    pub fn list_all_comments(&self) -> Result<Vec<CommentRecord>> {
        self.with_connection(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT id, videoid, author, text, likes, time_posted,
                       parent_comment_id, status_likedbycreator, reply_count
                FROM comments
                ORDER BY time_posted ASC
                "#,
            )?;

            let mut rows = stmt.query([])?;
            let mut comments = Vec::new();
            while let Some(row) = rows.next()? {
                comments.push(row_to_comment(row)?);
            }
            Ok(comments)
        })
    }

    fn fetch_videos_from(&self, table: &str) -> Result<Vec<VideoRecord>> {
        self.with_connection(|conn| {
            let mut stmt = conn.prepare(&format!(
                r#"
                SELECT videoid, title, description, likes, dislikes, views,
                       upload_date, author, subscriber_count, duration, duration_text,
                       channel_url, thumbnail_url, tags_json, thumbnails_json,
                       extras_json, sources_json
                FROM {table}
                ORDER BY upload_date DESC, rowid DESC
                "#
            ))?;

            let mut rows = stmt.query([])?;
            let mut records = Vec::new();
            while let Some(row) = rows.next()? {
                records.push(row_to_video_record(row)?);
            }
            Ok(records)
        })
    }

    fn fetch_single(&self, table: &str, videoid: &str) -> Result<Option<VideoRecord>> {
        self.with_connection(|conn| {
            let mut stmt = conn.prepare(&format!(
                r#"
                SELECT videoid, title, description, likes, dislikes, views,
                       upload_date, author, subscriber_count, duration, duration_text,
                       channel_url, thumbnail_url, tags_json, thumbnails_json,
                       extras_json, sources_json
                FROM {table}
                WHERE videoid = ?1
                "#
            ))?;

            let mut rows = stmt.query([videoid])?;
            if let Some(row) = rows.next()? {
                Ok(Some(row_to_video_record(row)?))
            } else {
                Ok(None)
            }
        })
    }
}

/// Converts a SQL row into a `VideoRecord`, deserializing the Vec/JSON fields.
fn row_to_video_record(row: &Row<'_>) -> Result<VideoRecord> {
    let tags_json: String = row.get("tags_json")?;
    let thumbnails_json: String = row.get("thumbnails_json")?;
    let extras_json: String = row.get("extras_json")?;
    let sources_json: String = row.get("sources_json")?;

    let tags: Vec<String> = serde_json::from_str(&tags_json).context("parsing stored tags JSON")?;
    let thumbnails: Vec<String> =
        serde_json::from_str(&thumbnails_json).context("parsing stored thumbnails JSON")?;
    let extras: serde_json::Value =
        serde_json::from_str(&extras_json).context("parsing stored extras JSON")?;
    let sources: Vec<VideoSource> =
        serde_json::from_str(&sources_json).context("parsing stored sources JSON")?;

    Ok(VideoRecord {
        videoid: row.get("videoid")?,
        title: row.get("title")?,
        description: row.get("description")?,
        likes: row.get("likes")?,
        dislikes: row.get("dislikes")?,
        views: row.get("views")?,
        upload_date: row.get("upload_date")?,
        author: row.get("author")?,
        subscriber_count: row.get("subscriber_count")?,
        duration: row.get("duration")?,
        duration_text: row.get("duration_text")?,
        channel_url: row.get("channel_url")?,
        thumbnail_url: row.get("thumbnail_url")?,
        tags,
        thumbnails,
        extras,
        sources,
    })
}

/// Converts a SQL row into a `CommentRecord` while normalizing the boolean flag
/// stored as an INTEGER in SQLite.
fn row_to_comment(row: &Row<'_>) -> Result<CommentRecord> {
    Ok(CommentRecord {
        id: row.get("id")?,
        videoid: row.get("videoid")?,
        author: row.get("author")?,
        text: row.get("text")?,
        likes: row.get("likes")?,
        time_posted: row.get("time_posted")?,
        parent_comment_id: row.get("parent_comment_id")?,
        status_likedbycreator: row
            .get::<_, i64>("status_likedbycreator")
            .map(|value| value != 0)?,
        reply_count: row.get("reply_count")?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
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
    fn create_store() -> Result<(tempfile::TempDir, MetadataStore, MetadataReader, PathBuf)> {
        let dir = tempdir()?;
        let path = dir.path().join("metadata/test.db");
        let store = MetadataStore::open(&path)?;
        let reader = MetadataReader::new(&path)?;
        Ok((dir, store, reader, path))
    }

    /// Validates that opening a store creates the DB file, turns on WAL mode and
    /// provisions every expected table/index. This guards against regressions in
    /// the bootstrap SQL.
    #[test]
    fn opens_store_and_creates_schema() -> Result<()> {
        let (_temp, _store, _reader, path) = create_store()?;
        assert!(path.exists(), "database file should be created");

        let conn = Connection::open(&path)?;
        let journal: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
        assert_eq!(journal.to_lowercase(), "wal");
        let synchronous: i64 = conn.query_row("PRAGMA synchronous", [], |row| row.get(0))?;
        assert!(
            synchronous >= 1,
            "synchronous should be NORMAL or stricter but was {synchronous}"
        );

        for table in ["videos", "shorts", "subtitles", "comments"] {
            let exists: Option<String> = conn
                .query_row(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name=?1",
                    [table],
                    |row| row.get(0),
                )
                .optional()?;
            assert_eq!(exists.as_deref(), Some(table));
        }

        for index in ["idx_comments_videoid", "idx_comments_parent"] {
            let exists: Option<String> = conn
                .query_row(
                    "SELECT name FROM sqlite_master WHERE type='index' AND name=?1",
                    [index],
                    |row| row.get(0),
                )
                .optional()?;
            assert_eq!(exists.as_deref(), Some(index));
        }
        Ok(())
    }

    /// Ensures that short-lived connections keep foreign_keys enforcement
    /// enabled so cascades behave consistently across helpers.
    #[test]
    fn reader_enforces_foreign_keys() -> Result<()> {
        let (_temp, _store, reader, _path) = create_store()?;
        reader.with_connection(|conn| {
            let flag: i64 = conn.query_row("PRAGMA foreign_keys", [], |row| row.get(0))?;
            assert_eq!(flag, 1);
            Ok(())
        })
    }

    /// Covers the insert/update path for long-form videos, ensuring JSON fields
    /// survive a round trip and updates override previous values as intended.
    #[test]
    fn upsert_video_roundtrip() -> Result<()> {
        let (_temp, store, reader, _path) = create_store()?;

        let mut record = sample_video("alpha");
        // First insertion should persist all provided metadata as-is.
        store.upsert_video(&record)?;

        let fetched = reader.get_video("alpha")?.expect("video fetched");
        assert_eq!(fetched.title, record.title);
        assert_eq!(fetched.tags, record.tags);
        assert_eq!(fetched.sources[0].format_id, "1080p");

        // Update a couple of fields and verify that ON CONFLICT rewrites them.
        record.title = "Updated".into();
        record.tags.push("review".into());
        store.upsert_video(&record)?;
        let updated = reader
            .get_video("alpha")?
            .expect("video fetched after update");
        assert_eq!(updated.title, "Updated");
        assert!(updated.tags.contains(&"review".into()));
        Ok(())
    }

    /// Mirrors the previous test but against the `shorts` table to guarantee
    /// feature parity between both content types.
    #[test]
    fn upsert_short_roundtrip() -> Result<()> {
        let (_temp, store, reader, _path) = create_store()?;

        let record = sample_video("shorty");
        // Short content uses the dedicated table but otherwise mirrors videos.
        store.upsert_short(&record)?;

        let shorts = reader.list_shorts()?;
        assert_eq!(shorts.len(), 1);
        assert_eq!(shorts[0].videoid, "shorty");
        Ok(())
    }

    /// Ensures subtitle collections get serialized to JSON and can be retrieved
    /// verbatim by the reader API.
    #[test]
    fn upsert_and_list_subtitles() -> Result<()> {
        let (_temp, store, reader, _path) = create_store()?;
        store.upsert_video(&sample_video("vid"))?;

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
        store.upsert_subtitles(&subtitles)?;

        let listed = reader.list_subtitles()?;
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].languages[0].code, "en");
        Ok(())
    }

    /// Exercises the transactional comment replacement flow so we never keep
    /// stale comment trees after a new download cycle.
    #[test]
    fn replace_comments_resets_previous_entries() -> Result<()> {
        let (_temp, mut store, reader, _path) = create_store()?;
        store.upsert_video(&sample_video("vid"))?;

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
        store.replace_comments("vid", &first)?;

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
        store.replace_comments("vid", &second)?;

        let fetched = reader.get_comments("vid")?;
        assert_eq!(fetched.len(), 1);
        assert_eq!(fetched[0].id, "2");
        assert!(!fetched[0].status_likedbycreator);
        Ok(())
    }

    /// Verifies that listing videos applies the desired ordering (newest first)
    /// even when dates differ, which is critical for deterministic feeds.
    #[test]
    fn list_videos_returns_sorted_records() -> Result<()> {
        let (_temp, store, reader, _path) = create_store()?;

        let mut old = sample_video("old");
        old.upload_date = Some("2023-01-01".into());
        store.upsert_video(&old)?;

        let mut new = sample_video("new");
        new.upload_date = Some("2024-05-01".into());
        store.upsert_video(&new)?;

        let videos = reader.list_videos()?;
        assert_eq!(videos.len(), 2);
        assert_eq!(videos[0].videoid, "new");
        assert_eq!(videos[1].videoid, "old");
        Ok(())
    }

    /// Reader helpers should gracefully return `None` when a record is missing.
    #[test]
    fn reader_returns_none_for_missing_entries() -> Result<()> {
        let (_temp, _store, reader, _path) = create_store()?;
        assert!(reader.get_video("ghost")?.is_none());
        assert!(reader.get_short("ghost")?.is_none());
        assert!(reader.get_subtitles("ghost")?.is_none());
        Ok(())
    }

    /// Listing shorts mirrors videos but must respect upload_date ordering.
    #[test]
    fn list_shorts_sorted_by_upload_date() -> Result<()> {
        let (_temp, store, reader, _path) = create_store()?;
        let mut older = sample_video("short-old");
        older.upload_date = Some("2023-05-01".into());
        store.upsert_short(&older)?;

        let mut newer = sample_video("short-new");
        newer.upload_date = Some("2024-06-01".into());
        store.upsert_short(&newer)?;

        let shorts = reader.list_shorts()?;
        assert_eq!(shorts.len(), 2);
        assert_eq!(shorts[0].videoid, "short-new");
        assert_eq!(shorts[1].videoid, "short-old");
        Ok(())
    }

    /// Subtitle upserts should overwrite existing rows rather than append.
    #[test]
    fn upsert_subtitles_overwrites_existing_languages() -> Result<()> {
        let (_temp, store, reader, _path) = create_store()?;
        store.upsert_video(&sample_video("vid-sub"))?;

        let initial = SubtitleCollection {
            videoid: "vid-sub".into(),
            languages: vec![SubtitleTrack {
                code: "en".into(),
                name: "English".into(),
                url: "https://cdn/en.vtt".into(),
                path: None,
            }],
        };
        store.upsert_subtitles(&initial)?;

        let updated = SubtitleCollection {
            videoid: "vid-sub".into(),
            languages: vec![SubtitleTrack {
                code: "fr".into(),
                name: "Français".into(),
                url: "https://cdn/fr.vtt".into(),
                path: Some("/subs/fr.vtt".into()),
            }],
        };
        store.upsert_subtitles(&updated)?;

        let fetched = reader.get_subtitles("vid-sub")?.expect("subtitles exist");
        assert_eq!(fetched.languages.len(), 1);
        assert_eq!(fetched.languages[0].code, "fr");
        Ok(())
    }

    /// Comments containing replies and flags should persist verbatim.
    #[test]
    fn replace_comments_preserves_replies_and_flags() -> Result<()> {
        let (_temp, mut store, reader, _path) = create_store()?;
        store.upsert_video(&sample_video("with-comments"))?;

        let mut parent = sample_comment("parent", "with-comments");
        parent.status_likedbycreator = true;
        let mut reply = sample_comment("child", "with-comments");
        reply.parent_comment_id = Some("parent".into());

        store.replace_comments("with-comments", &[parent.clone(), reply.clone()])?;

        let comments = reader.get_comments("with-comments")?;
        assert_eq!(comments.len(), 2);
        assert_eq!(comments[0].id, "parent");
        assert!(comments[0].status_likedbycreator);
        assert_eq!(comments[1].parent_comment_id.as_deref(), Some("parent"));
        Ok(())
    }

    /// list_all_comments should merge comments across videos ordered by timestamp.
    #[test]
    fn list_all_comments_orders_by_time() -> Result<()> {
        let (_temp, mut store, reader, _path) = create_store()?;
        store.upsert_video(&sample_video("video-one"))?;
        store.upsert_video(&sample_video("video-two"))?;

        let mut first = sample_comment("1", "video-one");
        first.time_posted = Some("2024-01-01T00:00:00Z".into());
        let mut second = sample_comment("2", "video-two");
        second.time_posted = Some("2024-01-01T00:05:00Z".into());
        let mut third = sample_comment("3", "video-one");
        third.time_posted = Some("2024-01-01T00:10:00Z".into());

        store.replace_comments("video-one", &[first.clone(), third.clone()])?;
        store.replace_comments("video-two", &[second.clone()])?;

        let all = reader.list_all_comments()?;
        assert_eq!(all.len(), 3);
        assert_eq!(all[0].id, "1");
        assert_eq!(all[1].id, "2");
        assert_eq!(all[2].id, "3");
        Ok(())
    }
}
