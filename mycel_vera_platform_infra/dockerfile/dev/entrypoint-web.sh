#!/bin/sh
set -e

echo "=========================================="
echo "Starting Mycel Vera Platform Web Dev Server"
echo "=========================================="
echo ""
echo "Architecture: mycel_core_web (shell) hosts mycel_vera_platform_web (pages)"
echo ""

# ─────────────────────────────────────────────
# Environment info
# ─────────────────────────────────────────────
echo "[Mycel] ROOT_MODULE_NAME: ${ROOT_MODULE_NAME:-not set}"
echo "[Mycel] SUBMODULES: ${SUBMODULES:-not set}"
echo "[Mycel] MYCEL_REGISTRY_PATH: ${MYCEL_REGISTRY_PATH:-not set}"
echo ""

# ─────────────────────────────────────────────
# Install dependencies
# ─────────────────────────────────────────────
cd /build

echo "[Mycel] Installing dependencies..."
npm install --prefer-offline 2>&1 | tail -5
echo "[Mycel] Dependencies installed."
echo "[Mycel] Installed modules: $(ls node_modules 2>/dev/null | wc -l) packages"

# ─────────────────────────────────────────────
# Symlink node_modules into source dirs so ESM resolution works
# (volume mount of src/ may override, so re-symlink is safe)
# ─────────────────────────────────────────────
ln -sfn /build/node_modules /build/mycel_core_web/node_modules
ln -sfn /build/node_modules /build/${ROOT_MODULE_NAME}_web/node_modules
echo "[Mycel] Symlinked node_modules into source directories."

# ─────────────────────────────────────────────
# Generate registry
# ─────────────────────────────────────────────
cd /build/mycel_core_web

echo "[Mycel] Generating mycel_registry.ts..."
node scripts/generate-registry.js

if [ -f "${MYCEL_REGISTRY_PATH}" ]; then
    echo "[Mycel] Registry generated at: ${MYCEL_REGISTRY_PATH}"
    echo ""
    echo "Registry contents:"
    echo "─────────────────────────────────────────"
    head -30 "${MYCEL_REGISTRY_PATH}"
    echo "..."
    echo "─────────────────────────────────────────"
else
    echo "[Mycel] Warning: Registry file not found at ${MYCEL_REGISTRY_PATH}"
fi

# ─────────────────────────────────────────────
# Start Vite dev server from mycel_core_web
# ─────────────────────────────────────────────
echo ""
echo "[Mycel] Starting Vite dev server from mycel_core_web..."
echo ""

exec npm run dev -- --host 0.0.0.0
