#!/usr/bin/env bash
# Vendored by dx/scripts/image-audit-sync from dx's .github/actions/image-audit/run.sh. Do not edit directly — change the source in dx and re-run image-audit-sync.
# Implementation for the image-audit composite action. See action.yaml for inputs.
#
# Reports two independent things per pinned image: whether a newer version exists,
# and whether the pinned reference carries fixable HIGH/CRITICAL vulnerabilities.
# Both are needed — a fully current image can still be vulnerable, and upgrading
# does not always remediate. Nothing is ever rewritten; see Decision
# https://github.com/orgs/qualithm/discussions/232.
set -euo pipefail

: "${MODE:?}"

WORKDIR="${WORKDIR:-$PWD}"
OUT="${OUT:-$PWD/image-audit-body.md}"
SEVERITY="${SEVERITY:-HIGH,CRITICAL}"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

cd "$WORKDIR"

# ---------------------------------------------------------------- enumerate
# pins.tsv columns: location <TAB> image <TAB> tag
: > "$work/pins.tsv"
: > "$work/charts.tsv"

case "$MODE" in
helm)
  for f in helm/*/values.yaml; do
    [[ -f "$f" ]] || continue
    chart="${f%%/values.yaml}"
    chart="${chart#helm/}"
    awk -v chart="$chart" '
      /repository:/ { r = $2 }
      /^[[:space:]]+tag:/ {
        t = $2; gsub(/"/, "", t)
        if (length(r) > 0) { print chart "\t" r "\t" t; r = "" }
      }
    ' "$f" >> "$work/pins.tsv"
  done
  # Upstream subchart pins live in Chart.yaml dependencies, not values.yaml.
  for f in helm/*/Chart.yaml; do
    [[ -f "$f" ]] || continue
    chart="${f%%/Chart.yaml}"
    chart="${chart#helm/}"
    awk -v chart="$chart" '
      /^[[:space:]]*-[[:space:]]*name:/ { n = $3; v = ""; u = "" }
      /^[[:space:]]+version:/ { v = $2; gsub(/"/, "", v) }
      /^[[:space:]]+repository:/ {
        u = $2; gsub(/"/, "", u)
        if (length(n) > 0 && length(v) > 0) { print chart "\t" n "\t" v "\t" u; n = "" }
      }
    ' "$f" >> "$work/charts.tsv"
  done
  ;;
docker)
  while IFS= read -r f; do
    awk -v file="$f" '
      /^FROM / {
        ref = $2
        # Only digest-pinned bases are trackable here: the tag never floats, so a
        # rebuilt upstream is invisible without comparing digests.
        if (index(ref, "@sha256:") > 0) {
          split(ref, parts, "@")
          n = split(parts[1], nt, ":")
          image = nt[1]
          for (i = 2; i < n; i++) image = image ":" nt[i]
          print file "\t" image "\t" nt[n] "@" parts[2]
        }
      }
    ' "$f" >> "$work/pins.tsv"
  done < <(find . -name 'Dockerfile*' -not -path './node_modules/*' -not -path './.git/*' | sort)
  ;;
*)
  echo "image-audit: unknown mode: ${MODE}" >&2
  exit 1
  ;;
esac

# First-party images are pinned to sha-<commit> by the release pipeline, not to an
# upstream version, so version tracking is meaningless for them. The same pin can
# appear twice in one values.yaml (a chart and its sidecar); one row is enough.
grep -v 'ghcr.io/qualithm' "$work/pins.tsv" | awk 'seen[$0]++ == 0' > "$work/pins.filtered" || true
mv "$work/pins.filtered" "$work/pins.tsv"

# ------------------------------------------------------------------ resolve
# Bound every lookup to the pin's own major. Two filters are unsafe here and both
# were observed in practice: `regex` sorts lexically (proposing 1.9.15 as newer
# than 1.29.1), and an unbounded semver pattern picks up numeric CI build-ID tags
# that parse as enormous valid majors (grafana publishes 9799770991).
{
  echo "name: image drift"
  echo "sources:"
  i=0
  while IFS=$'\t' read -r _loc image tag; do
    i=$((i + 1))
    # A digest pin can't drift by tag, only by the tag being rebuilt underneath it,
    # so ask for the tag's current digest rather than a newer version.
    case "$tag" in
    *@sha256:*)
      echo "  s${i}:"
      echo "    kind: dockerdigest"
      echo "    spec: { image: ${image}, tag: \"${tag%%@*}\" }"
      continue
      ;;
    esac
    base="${tag#v}"
    suffix=""
    case "$base" in *-*) suffix="-${base#*-}"; base="${base%%-*}" ;; esac
    major="${base%%.*}"
    case "$major" in ''|*[!0-9]*) continue ;; esac
    echo "  s${i}:"
    echo "    kind: dockerimage"
    echo "    spec: { image: ${image}, versionfilter: { kind: semver, pattern: \"${major}.x\" } }"
    if [[ -n "$suffix" ]]; then
      echo "    transformers:"
      echo "      - addsuffix: \"${suffix}\""
    fi
    # Unbounded lookup purely to surface that a new major exists; never proposed as
    # the upgrade, and filtered at render time because some registries publish
    # numeric build-ID tags that are valid semver majors.
    echo "  sM${i}:"
    echo "    kind: dockerimage"
    echo "    spec: { image: ${image}, versionfilter: { kind: semver, pattern: \"*\" } }"
  done < "$work/pins.tsv"
  j=0
  while IFS=$'\t' read -r _chart name version url; do
    j=$((j + 1))
    case "$url" in oci://*) continue ;; esac
    base="${version#v}"
    major="${base%%.*}"
    case "$major" in ''|*[!0-9]*) continue ;; esac
    echo "  c${j}:"
    echo "    kind: helmchart"
    echo "    spec: { url: ${url%/}, name: ${name}, versionfilter: { kind: semver, pattern: \"${major}.x\" } }"
    echo "  cM${j}:"
    echo "    kind: helmchart"
    echo "    spec: { url: ${url%/}, name: ${name}, versionfilter: { kind: semver, pattern: \"*\" } }"
  done < "$work/charts.tsv"
} > "$work/manifest.yaml"

# updatecli exits 0 whether or not anything is out of date, so the result has to
# come from its output. A transformed value supersedes the raw one it came from.
updatecli diff --config "$work/manifest.yaml" 2>&1 | awk '
  /^source: / { id = $2 }
  /found matching pattern/ {
    match($0, /"[^"]+"/); v[id] = substr($0, RSTART + 1, RLENGTH - 2)
  }
  /is found from repository/ {
    match($0, /version "[^"]+"/)
    s = substr($0, RSTART + 9, RLENGTH - 10); gsub(/"/, "", s); v[id] = s
  }
  # dockerdigest reports a fully-qualified ref rather than a version, e.g.
  # "resolved to digest index.docker.io/library/alpine:3.24@sha256:...".
  /resolved to digest/ {
    match($0, /[^ ]+@sha256:[a-f0-9]+/)
    full = substr($0, RSTART, RLENGTH)
    split(full, ab, "@")
    m = split(ab[1], cs, ":")
    v[id] = cs[m] "@" ab[2]
  }
  /Result correctly transformed/ { n = split($0, p, "\""); v[id] = p[n - 1] }
  END { for (k in v) print k "\t" v[k] }
' > "$work/resolved.tsv" || true

resolved_for() { awk -F'\t' -v k="$1" '$1 == k { print $2 }' "$work/resolved.tsv" | head -1; }

# Advisory only. A candidate counts as a next major when it is genuinely larger and
# still looks like a release number — some registries publish numeric build-ID tags
# (grafana ships 9799770991) that parse as valid, enormous semver majors.
next_major() {
  local current="$1" candidate="$2" cur cand
  [[ -n "$candidate" ]] || return 0
  cur="${current#v}"; cur="${cur%%-*}"; cur="${cur%%.*}"
  cand="${candidate#v}"; cand="${cand%%-*}"; cand="${cand%%.*}"
  case "$cur" in ''|*[^0-9]*) return 0 ;; esac
  case "$cand" in ''|*[^0-9]*) return 0 ;; esac
  if [[ "$cand" -gt "$cur" ]] && [[ ${#cand} -le $((${#cur} + 1)) ]]; then
    printf '%s' "$candidate"
  fi
}

# Scan the exact reference that is deployed: a digest pin must be scanned by digest,
# since its tag may since have been rebuilt to different content.
ref_for() {
  local image="$1" tag="$2"
  case "$tag" in
  *@sha256:*) printf '%s@%s' "$image" "${tag#*@}" ;;
  *) printf '%s:%s' "$image" "$tag" ;;
  esac
}

# ---------------------------------------------------------------- scan
# Counts are cached per reference: alpine/k8s alone is pinned by three charts, and
# each scan is an image pull.
scan_count() {
  local ref="$1" key
  key="$(printf '%s' "$ref" | tr -c 'A-Za-z0-9' '_')"
  if [[ -f "$work/cve-$key" ]]; then cat "$work/cve-$key"; return; fi
  local n
  n="$(trivy image --scanners vuln --severity "$SEVERITY" --ignore-unfixed \
    --quiet --format json "$ref" 2>/dev/null | grep -c '"VulnerabilityID"' || true)"
  [[ -n "$n" ]] || n="?"
  printf '%s' "$n" > "$work/cve-$key"
  printf '%s' "$n"
}

# ---------------------------------------------------------------- render
behind=0
vulnerable=0
rows="$work/rows.md"
: > "$rows"

i=0
while IFS=$'\t' read -r loc image tag; do
  i=$((i + 1))
  latest="$(resolved_for "s${i}")"

  now="$(scan_count "$(ref_for "$image" "$tag")")"
  [[ "$now" =~ ^[0-9]+$ ]] && [[ "$now" -gt 0 ]] && vulnerable=$((vulnerable + 1))

  after="—"
  major_note="$(next_major "$tag" "$(resolved_for "sM${i}")")"
  [[ -n "$major_note" ]] || major_note="—"
  if [[ -n "$latest" && "$latest" != "$tag" ]]; then
    behind=$((behind + 1))
    after="$(scan_count "$(ref_for "$image" "$latest")")"
    printf '| %s | `%s` | %s | **%s** | %s | %s | %s |\n' \
      "$loc" "$image" "$tag" "$latest" "$major_note" "$now" "$after" >> "$rows"
  elif [[ "$now" =~ ^[0-9]+$ ]] && [[ "$now" -gt 0 ]]; then
    printf '| %s | `%s` | %s | _current_ | %s | %s | %s |\n' \
      "$loc" "$image" "$tag" "$major_note" "$now" "$after" >> "$rows"
  fi
done < "$work/pins.tsv"

chart_rows="$work/charts.md"
: > "$chart_rows"
j=0
while IFS=$'\t' read -r chart name version url; do
  j=$((j + 1))
  latest="$(resolved_for "c${j}")"
  major_note="$(next_major "$version" "$(resolved_for "cM${j}")")"
  [[ -n "$major_note" ]] || major_note="—"
  if [[ -n "$latest" && "$latest" != "$version" ]]; then
    behind=$((behind + 1))
    printf '| %s | `%s` | %s | **%s** | %s |\n' "$chart" "$name" "$version" "$latest" "$major_note" >> "$chart_rows"
  elif [[ "$major_note" != "—" ]]; then
    printf '| %s | `%s` | %s | _current_ | %s |\n' "$chart" "$name" "$version" "$major_note" >> "$chart_rows"
  fi
done < "$work/charts.tsv"

{
  if [[ -s "$rows" ]]; then
    echo "### Images"
    echo
    echo "| Location | Image | Pinned | Latest | Next major | CVEs now | CVEs after upgrade |"
    echo "|---|---|---|---|---|---|---|"
    cat "$rows"
    echo
  fi
  if [[ -s "$chart_rows" ]]; then
    echo "### Chart dependencies"
    echo
    echo "| Chart | Dependency | Pinned | Latest | Next major |"
    echo "|---|---|---|---|---|"
    cat "$chart_rows"
    echo
  fi
  echo "### Notes"
  echo
  echo "- Counts are fixable ${SEVERITY} only (\`--ignore-unfixed\`), so every one has a fix available."
  echo "- \`CVEs after upgrade\` is the candidate's count: equal or higher means upgrading is not the remedy."
  echo "- A row marked _current_ is on the newest version and still vulnerable — no upgrade will help."
  echo "- Nothing is changed automatically; upgrades are a human call (Decision"
  echo "  <https://github.com/orgs/qualithm/discussions/232>)."
  if grep -q 'oci://' "$work/charts.tsv" 2>/dev/null; then
    echo "- OCI-hosted chart dependencies are not version-checked yet and are omitted."
  fi
} > "$OUT"

if [[ -s "$rows" || -s "$chart_rows" ]]; then
  echo "clean=false"
else
  echo "clean=true"
fi
# One output per line: GITHUB_OUTPUT splits at the first '=', so a combined
# line would fold the vulnerable count into the behind value.
echo "behind=${behind}"
echo "vulnerable=${vulnerable}"
