.PHONY: desktop-build

desktop-build:
	cd ui/lianghua_web && npm ci
	cd ui/lianghua_web && npm run tauri build
