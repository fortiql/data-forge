# 🛑 Troubleshooting

Why: Quick fixes for common local issues.

## 🚧 Build hangs on “resolving provenance for metadata file”

- Disable default attestations for this session:
  - `export BUILDX_NO_DEFAULT_ATTESTATIONS=1`
  - `docker compose --profile core build --progress=plain`

- Or bypass BuildKit entirely for the run:
  - `DOCKER_BUILDKIT=0 docker compose --profile core up --build`

- Refresh buildx and prune cache (optional):
  - `docker buildx inspect --bootstrap`
  - `docker buildx prune -af`

## 🔌 Ports already in use

- Stop previous stacks: `docker compose down`
- Find process: Linux/macOS `lsof -i :PORT`, Windows `netstat -aon | findstr :PORT`

## 📝 Notes

- Share `docker version`, `docker buildx version`, `docker compose version` if issues persist.

