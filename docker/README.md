# OneDis container image

Run `scripts/bootstrap-dependencies.sh` first. The build then needs the parent directory as context
because OneDis uses the sibling Tantivy checkout:

```bash
cp onedis/docker/workspace.dockerignore .dockerignore
docker build --pull -f onedis/docker/Dockerfile -t onedis:local .
docker run --name onedis --stop-timeout 40 \
  -p 6379:6379 -p 127.0.0.1:9121:9121 \
  -v onedis-data:/var/lib/onedis \
  --read-only --tmpfs /tmp:rw,noexec,nosuid,size=64m \
  onedis:local
```

The image runs as UID/GID `10001`, stores kv-engine state under `/var/lib/onedis`, checks
`/readyz`, and sends SIGTERM directly to `onedis-server`. Mount a replacement configuration at
`/etc/onedis/onedis.toml` when using object-backed tablet storage.

Do not use Redis `SAVE`, `BGSAVE`, RDB, or AOF procedures with OneDis. Backup and recovery are
owned by the configured kv-engine/tablet-store deployment.
