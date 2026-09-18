.PHONY: desktop-build

# Reproducible desktop build from a fresh clone: restore the locked frontend
# dependencies before invoking the locally installed Tauri CLI.
desktop-build:
	cd ui/lianghua_web && npm ci
	cd ui/lianghua_web && npm run tauri build
