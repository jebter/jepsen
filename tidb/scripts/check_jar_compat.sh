#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
usage: scripts/check_jar_compat.sh [--max-major <n>] [--forbid-symbol <symbol>]... <jar-or-url>

Checks whether a standalone Jepsen jar is safe to run on an older JVM.

Default checks:
- every root class file stays at or below Java 8 bytecode level (major 52)
- no root class references java.util sequenced collection interfaces introduced after Java 8

Notes:
- local file paths and http/https URLs are both supported
- when the input is a URL, the script validates the final published artifact
- module-info.class and META-INF/versions/* are ignored because they are not
  part of the Java 8 classpath surface
- repeat --forbid-symbol to extend the default forbidden symbol list
EOF
}

require_cmd() {
  local name="$1"
  if ! command -v "$name" >/dev/null 2>&1; then
    echo "missing required command: $name" >&2
    exit 1
  fi
}

is_http_url() {
  local value="$1"
  [[ "$value" == http://* || "$value" == https://* ]]
}

print_failure_block() {
  local title="$1"
  shift
  local -a failures=("$@")
  local max_lines=20
  local i=0

  [[ ${#failures[@]} -eq 0 ]] && return 0

  echo "$title" >&2
  for failure in "${failures[@]}"; do
    echo "  - $failure" >&2
    i=$((i + 1))
    if [[ "$i" -ge "$max_lines" ]]; then
      local remaining=$(( ${#failures[@]} - max_lines ))
      if [[ "$remaining" -gt 0 ]]; then
        echo "  - ... and $remaining more" >&2
      fi
      break
    fi
  done
}

MAX_MAJOR=52
FORBID_SYMBOLS=(
  "java/util/SequencedCollection"
  "java/util/SequencedMap"
  "java/util/SequencedSet"
)
JAR_PATH=""
JAR_SOURCE=""
JAR_INPUT_PATH=""
BUILD_JDK=""
MAIN_CLASS=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --max-major)
      [[ $# -ge 2 ]] || {
        echo "--max-major requires a value" >&2
        usage >&2
        exit 2
      }
      MAX_MAJOR="$2"
      shift 2
      ;;
    --forbid-symbol)
      [[ $# -ge 2 ]] || {
        echo "--forbid-symbol requires a value" >&2
        usage >&2
        exit 2
      }
      FORBID_SYMBOLS+=("$2")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
    *)
      if [[ -n "$JAR_PATH" ]]; then
        echo "unexpected extra argument: $1" >&2
        usage >&2
        exit 2
      fi
      JAR_PATH="$1"
      shift
      ;;
  esac
done

if [[ $# -gt 0 ]]; then
  if [[ -n "$JAR_PATH" ]]; then
    echo "unexpected extra argument: $1" >&2
    usage >&2
    exit 2
  fi
  JAR_PATH="$1"
  shift
fi

if [[ -z "$JAR_PATH" ]]; then
  usage >&2
  exit 2
fi

case "$MAX_MAJOR" in
  ''|*[!0-9]*)
    echo "--max-major must be a non-negative integer: $MAX_MAJOR" >&2
    exit 2
    ;;
esac

require_cmd curl
require_cmd unzip
require_cmd find
require_cmd od
require_cmd grep
require_cmd awk

TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/jar-compat.XXXXXX")"
trap 'rm -rf "$TMP_DIR"' EXIT

if is_http_url "$JAR_PATH"; then
  JAR_SOURCE="$JAR_PATH"
  JAR_INPUT_PATH="$TMP_DIR/input.jar"
  echo "downloading jar from URL: $JAR_SOURCE" >&2
  curl -fsSL "$JAR_SOURCE" -o "$JAR_INPUT_PATH"
else
  if [[ ! -f "$JAR_PATH" ]]; then
    echo "jar not found: $JAR_PATH" >&2
    exit 1
  fi
  JAR_SOURCE="$JAR_PATH"
  JAR_INPUT_PATH="$JAR_PATH"
fi

EXTRACT_DIR="$TMP_DIR/extracted"
mkdir -p "$EXTRACT_DIR"
unzip -qq "$JAR_INPUT_PATH" -d "$EXTRACT_DIR"

if [[ -f "$EXTRACT_DIR/META-INF/MANIFEST.MF" ]]; then
  BUILD_JDK="$(awk -F': ' 'tolower($1)=="build-jdk"{print $2}' "$EXTRACT_DIR/META-INF/MANIFEST.MF" | head -n 1)"
  MAIN_CLASS="$(awk -F': ' 'tolower($1)=="main-class"{print $2}' "$EXTRACT_DIR/META-INF/MANIFEST.MF" | head -n 1)"
fi

CLASS_COUNT=0
MAX_SEEN_MAJOR=0
MAJOR_FAILURES=()
SYMBOL_FAILURES=()

while IFS= read -r -d '' class_file; do
  rel_path="${class_file#"$EXTRACT_DIR"/}"
  case "$rel_path" in
    META-INF/versions/*|module-info.class)
      continue
      ;;
  esac

  CLASS_COUNT=$((CLASS_COUNT + 1))

  bytes=($(od -An -t u1 -N 8 "$class_file"))
  if [[ ${#bytes[@]} -lt 8 ]]; then
    MAJOR_FAILURES+=("$rel_path (unable to read class header)")
    continue
  fi

  major=$(( bytes[6] * 256 + bytes[7] ))
  if [[ "$major" -gt "$MAX_SEEN_MAJOR" ]]; then
    MAX_SEEN_MAJOR="$major"
  fi
  if [[ "$major" -gt "$MAX_MAJOR" ]]; then
    MAJOR_FAILURES+=("$rel_path (major=$major, allowed<=$MAX_MAJOR)")
  fi

  if [[ ${#FORBID_SYMBOLS[@]} -gt 0 ]]; then
    for symbol in "${FORBID_SYMBOLS[@]}"; do
      if LC_ALL=C grep -aFq "$symbol" "$class_file"; then
        SYMBOL_FAILURES+=("$rel_path ($symbol)")
        break
      fi
    done
  fi
done < <(find "$EXTRACT_DIR" -type f -name '*.class' -print0 | sort -z)

if [[ "$CLASS_COUNT" -eq 0 ]]; then
  echo "no class files found in jar: $JAR_SOURCE" >&2
  exit 1
fi

if [[ ${#MAJOR_FAILURES[@]} -gt 0 || ${#SYMBOL_FAILURES[@]} -gt 0 ]]; then
  echo "jar compatibility check failed: $JAR_SOURCE" >&2
  echo "classes scanned: $CLASS_COUNT" >&2
  echo "max major seen: $MAX_SEEN_MAJOR" >&2
  if [[ -n "$BUILD_JDK" ]]; then
    echo "manifest Build-Jdk: $BUILD_JDK" >&2
  fi
  if [[ -n "$MAIN_CLASS" ]]; then
    echo "manifest Main-Class: $MAIN_CLASS" >&2
  fi
  echo "module-info.class and META-INF/versions/* were skipped" >&2
  if [[ ${#MAJOR_FAILURES[@]} -gt 0 ]]; then
    print_failure_block "bytecode level violations:" "${MAJOR_FAILURES[@]}"
  fi
  if [[ ${#SYMBOL_FAILURES[@]} -gt 0 ]]; then
    print_failure_block "forbidden runtime symbol violations:" "${SYMBOL_FAILURES[@]}"
  fi
  exit 1
fi

echo "jar compatibility check passed: $JAR_SOURCE"
echo "classes scanned: $CLASS_COUNT"
echo "max major seen: $MAX_SEEN_MAJOR"
echo "max allowed major: $MAX_MAJOR"
if [[ -n "$BUILD_JDK" ]]; then
  echo "manifest Build-Jdk: $BUILD_JDK"
fi
if [[ -n "$MAIN_CLASS" ]]; then
  echo "manifest Main-Class: $MAIN_CLASS"
fi
echo "module-info.class and META-INF/versions/* were skipped"
if [[ ${#FORBID_SYMBOLS[@]} -gt 0 ]]; then
  printf 'forbidden symbols checked: %s\n' "$(IFS=', '; echo "${FORBID_SYMBOLS[*]}")"
fi
