#!/usr/bin/env bash
# ITL-1403: a store written by deltalake OLD, read and appended by NEW, the reverse, and a
# control written by OLD alone; every dump read by both versions; dumps compared.
# usage: interop.sh OLD_PYTHON NEW_PYTHON WORKDIR
set -euo pipefail
OLD="$1"; NEW="$2"; W="$3"; HERE="$(cd "$(dirname "$0")" && pwd)"
export RUST_LOG=off
rm -rf "$W"; mkdir -p "$W"
for case in old-new new-old old-old new-new; do
  first=${case%-*}; second=${case#*-}
  P1=$([ "$first" = old ] && echo "$OLD" || echo "$NEW"); P2=$([ "$second" = old ] && echo "$OLD" || echo "$NEW")
  "$P1" "$HERE/interop.py" run "$W/$case" first
  "$P2" "$HERE/interop.py" run "$W/$case" second
  "$OLD" "$HERE/interop.py" dump "$W/$case" "$W/$case.read-by-old.json"
  "$NEW" "$HERE/interop.py" dump "$W/$case" "$W/$case.read-by-new.json"
done
python3 "$HERE/compare.py" "$W/old-old.read-by-old.json" "$W"/*.json
