#![forbid(unsafe_code)]

//! Public entry point for the reusable ViewTube Rust crate.
//!
//! The crate is intentionally small; it mostly exposes the metadata module so
//! binaries can share struct definitions and database helpers.

pub mod config;
pub mod metadata;
pub mod security;
