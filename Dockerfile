FROM archlinux:latest AS builder
RUN pacman -Syu --noconfirm --needed rust cargo base-devel
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release

FROM archlinux:latest
RUN pacman -Syu --noconfirm --needed ca-certificates yt-dlp
RUN useradd -r -u 10001 -m -d /app newtube
WORKDIR /app
COPY --from=builder /app/target/release/backend /usr/local/bin/backend
COPY --from=builder /app/target/release/download_channel /usr/local/bin/download_channel
COPY --from=builder /app/target/release/routine_update /usr/local/bin/routine_update
COPY index.html app.js pageHome.js pageViewer.js pageAdmin.js userData.js styles.css sw.js Roboto-*.ttf /app/www/
RUN mkdir -p /data/media /app/www \
    && printf 'MEDIA_ROOT="/data/media"\nWWW_ROOT="/app/www"\nNEWTUBE_PORT="8080"\nNEWTUBE_HOST="0.0.0.0"\nNEWTUBE_MISSING_MEDIA_BEHAVIOR="404"\n' > /app/.env \
    && chown -R newtube:newtube /app /data/media
USER newtube
EXPOSE 8080
CMD ["backend"]
