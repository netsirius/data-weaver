#!/bin/bash

echo "Por favor, ingrese su token de acceso para Azure Artifacts:"
read -s AZURE_TOKEN

# URL de tu JAR en Azure Artifacts
AZURE_JAR_URL=""
JAR_NAME="data-weaver.jar"
INSTALL_DIR="$HOME/.data-weaver"
BIN_DIR="$HOME/bin"

# Crear directorios si no existen
mkdir -p "$INSTALL_DIR"
mkdir -p "$BIN_DIR"

# Descargar el JAR
echo "Descargando Data Weaver..."
# curl -u :$AZURE_TOKEN -o "$INSTALL_DIR/$JAR_NAME" "$AZURE_JAR_URL"

# Hacer el JAR ejecutable
chmod +x "$INSTALL_DIR/$JAR_NAME"

# Crear un script de envoltura en BIN_DIR
echo "#!/bin/bash" > "$BIN_DIR/weaver"
echo "java -jar $INSTALL_DIR/$JAR_NAME \"\$@\"" >> "$BIN_DIR/weaver"
chmod +x "$BIN_DIR/weaver"

# Agregar BIN_DIR al PATH si aún no está
if [[ ":$PATH:" != *":$BIN_DIR:"* ]]; then
    echo "export PATH=\$PATH:$BIN_DIR" >> "$HOME/.bashrc"
    source "$HOME/.bashrc"
fi

echo "Instalación completada. Data Weaver está disponible como 'weaver' en el PATH."
