#!/bin/bash

# YouTube Channel Downloader Script
# Downloads all videos, shorts, subtitles, and thumbnails from a YouTube channel

set -e  # Exit on error

# Configuration
CHANNEL_URL="$1"
BASE_DIR="/yt"
VIDEOS_DIR="$BASE_DIR/videos"
SHORTS_DIR="$BASE_DIR/shorts"
SUBTITLES_DIR="$BASE_DIR/subtitles"
THUMBNAILS_DIR="$BASE_DIR/thumbnails"
ARCHIVE_FILE="$BASE_DIR/download-archive.txt"
COOKIES_FILE="$BASE_DIR/cookies.txt"  # Optional: for age-restricted content

# Create directories
mkdir -p "$VIDEOS_DIR" "$SHORTS_DIR" "$SUBTITLES_DIR" "$THUMBNAILS_DIR"

# Check if channel URL provided
if [ -z "$CHANNEL_URL" ]; then
    echo "Usage: $0 <channel_url>"
    echo "Example: $0 https://www.youtube.com/@channelname"
    exit 1
fi

# Check if yt-dlp is installed
if ! command -v yt-dlp &> /dev/null; then
    echo "Error: yt-dlp is not installed"
    echo "Install with: pip install yt-dlp --break-system-packages"
    exit 1
fi

echo "==================================="
echo "YouTube Channel Downloader"
echo "==================================="
echo "Channel: $CHANNEL_URL"
echo "Base directory: $BASE_DIR"
echo ""

# Function to download all formats for a single video
download_video_all_formats() {
    local video_id="$1"
    local output_dir="$2"
    local video_url="https://www.youtube.com/watch?v=${video_id}"
    
    echo "Processing video: $video_id"
    
    # Create video directory
    mkdir -p "$output_dir/$video_id"
    
    # Download metadata (info.json) without embedding it
    yt-dlp \
        --write-info-json \
        --write-description \
        --write-thumbnail \
        --skip-download \
        --output "$output_dir/$video_id/$video_id" \
        "$video_url" 2>/dev/null || true
    
    # Download subtitles
    yt-dlp \
        --write-sub \
        --write-auto-sub \
        --sub-langs "all" \
        --sub-format "vtt" \
        --convert-subs "vtt" \
        --skip-download \
        --output "$SUBTITLES_DIR/$video_id/$video_id" \
        "$video_url" 2>/dev/null || true
    
    # Download thumbnail
    yt-dlp \
        --write-thumbnail \
        --skip-download \
        --output "$THUMBNAILS_DIR/$video_id/$video_id" \
        "$video_url" 2>/dev/null || true
    
    # Get list of all available formats
    local formats=$(yt-dlp -F "$video_url" 2>/dev/null | grep -E "^[0-9]+" | awk '{print $1}')
    
    # Download each format individually
    for format_id in $formats; do
        echo "  Downloading format: $format_id"
        
        yt-dlp \
            --format "$format_id" \
            --output "$output_dir/$video_id/${video_id}_${format_id}.%(ext)s" \
            --no-embed-metadata \
            --no-embed-subs \
            --no-embed-thumbnail \
            --no-overwrites \
            --continue \
            --ignore-errors \
            --no-warnings \
            "$video_url" 2>/dev/null || echo "    Failed to download format $format_id"
    done
    
    echo "  Completed: $video_id"
}

# Function to get all video IDs from channel
get_video_ids() {
    local url="$1"
    local filter="$2"
    
    yt-dlp \
        --flat-playlist \
        --get-id \
        --match-filter "$filter" \
        --ignore-errors \
        "$url" 2>/dev/null
}

# Download regular videos
download_videos() {
    echo "Getting list of regular videos..."
    
    local video_ids=$(get_video_ids "$CHANNEL_URL/videos" "!is_live & original_url!*=/shorts/")
    
    if [ -z "$video_ids" ]; then
        echo "No videos found"
        return
    fi
    
    local count=$(echo "$video_ids" | wc -l)
    echo "Found $count videos"
    echo ""
    
    local current=0
    while IFS= read -r video_id; do
        ((current++))
        
        # Check if already in archive
        if grep -q "$video_id" "$ARCHIVE_FILE" 2>/dev/null; then
            echo "[$current/$count] Skipping $video_id (already downloaded)"
            continue
        fi
        
        echo "[$current/$count] Downloading $video_id"
        download_video_all_formats "$video_id" "$VIDEOS_DIR"
        
        # Add to archive
        echo "youtube $video_id" >> "$ARCHIVE_FILE"
        
    done <<< "$video_ids"
    
    echo ""
    echo "Regular videos download complete!"
}

# Download shorts
download_shorts() {
    echo "Getting list of shorts..."
    
    local short_ids=$(get_video_ids "$CHANNEL_URL/shorts" "original_url*=/shorts/")
    
    if [ -z "$short_ids" ]; then
        echo "No shorts found"
        return
    fi
    
    local count=$(echo "$short_ids" | wc -l)
    echo "Found $count shorts"
    echo ""
    
    local current=0
    while IFS= read -r short_id; do
        ((current++))
        
        # Check if already in archive
        if grep -q "$short_id" "$ARCHIVE_FILE" 2>/dev/null; then
            echo "[$current/$count] Skipping $short_id (already downloaded)"
            continue
        fi
        
        echo "[$current/$count] Downloading $short_id"
        download_video_all_formats "$short_id" "$SHORTS_DIR"
        
        # Add to archive
        echo "youtube $short_id" >> "$ARCHIVE_FILE"
        
    done <<< "$short_ids"
    
    echo ""
    echo "Shorts download complete!"
}

# Main execution
echo "Starting download process..."
echo ""

# Download videos
download_videos

# Download shorts
download_shorts

echo ""
echo "==================================="
echo "Download complete!"
echo "==================================="
echo "Videos: $VIDEOS_DIR"
echo "Shorts: $SHORTS_DIR"
echo "Subtitles: $SUBTITLES_DIR"
echo "Thumbnails: $THUMBNAILS_DIR"
echo "Archive: $ARCHIVE_FILE"
echo ""
echo "Metadata files:"
echo "  - <video_id>.info.json (video metadata)"
echo "  - <video_id>.description (video description)"
echo "  - <video_id>.jpg (thumbnail)"
echo ""
echo "Next steps:"
echo "1. Download likes/dislikes data separately"
echo "2. Download comments data separately"
echo "3. Process .info.json files to populate IndexedDB"
