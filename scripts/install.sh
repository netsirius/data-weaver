#!/bin/bash
set -e
VERSION="${WEAVER_VERSION:-latest}"
INSTALL_DIR="${WEAVER_INSTALL_DIR:-$HOME/.local/bin}"
GITHUB_REPO="netsirius/data-weaver"
echo "Installing Data Weaver..."
mkdir -p "$INSTALL_DIR"
if [ "$VERSION" = "latest" ]; then
  DOWNLOAD_URL="https://github.com/$GITHUB_REPO/releases/latest/download/data-weaver.jar"
else
  DOWNLOAD_URL="https://github.com/$GITHUB_REPO/releases/download/v$VERSION/data-weaver.jar"
fi
echo "Downloading from: $DOWNLOAD_URL"
curl -fsSL "$DOWNLOAD_URL" -o "$INSTALL_DIR/data-weaver.jar"
cat > "$INSTALL_DIR/weaver" << 'WSCRIPT'
#!/bin/bash
exec java ${JAVA_OPTS:--Xmx1g} -jar "$(dirname "$0")/data-weaver.jar" "$@"
WSCRIPT
chmod +x "$INSTALL_DIR/weaver"
echo ""
echo "Data Weaver installed!"
echo "  Location: $INSTALL_DIR/weaver"
echo "  Add to PATH: export PATH=\"$INSTALL_DIR:\$PATH\""
echo "  Get started: weaver init my-project"
