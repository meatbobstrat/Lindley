#!/usr/bin/env bash
# Start a packaged Lindley (the command given), on scratch folders: check it answers and serves
# its UI, then quit it from the app, as a person would. Used by workflows/installers.yml.
#
#   .github/smoke.sh build/.../Lindley.exe
set -euo pipefail

T="${RUNNER_TEMP:-/tmp}/lindley-smoke"
command -v cygpath > /dev/null && T="$(cygpath -m "$T")"  # Windows: a path JSON can hold
PORT="${PORT:-8797}"
url="http://127.0.0.1:$PORT"
rm -rf "$T" && mkdir -p "$T/inbox"
cat > "$T/settings.json" << EOF
{"watch_folders": ["$T/inbox"], "processing_dir": "$T/processing",
 "quarantine_dir": "$T/quarantine", "library_dir": "$T/library",
 "db_path": "$T/data/lindley.db"}
EOF

"$@" --no-browser --no-tray --port "$PORT" --settings "$T/settings.json" &
lindley=$!
trap 'echo "--- its log"; cat "$T/data/logs/lindley.log" 2> /dev/null || true' EXIT

for _ in $(seq 1 120); do
  curl -sf "$url/api/health" > /dev/null && break
  kill -0 "$lindley" 2> /dev/null || { echo "Lindley stopped as it started"; exit 1; }
  sleep 0.5
done
curl -sf "$url/api/health" && echo
curl -sf "$url/inbox" | grep -q '<div id="root">' || { echo "No UI at $url/inbox"; exit 1; }
curl -sf -X POST "$url/api/quit" && echo
wait "$lindley"
echo "Started, served its UI, and quit"
