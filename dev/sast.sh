#!/usr/bin/env bash
# Note: CI's semgrep job is report-only (`|| true`, per plan); this local run gates on ERROR.
# Local SAST pipeline - the same scanners and thresholds as
# .github/workflows/security.yml, run via Docker (plus native cargo-deny and
# cargo-cyclonedx).
#
# Every tool reads its config from the repo root (deny.toml, .hadolint.yaml,
# .gitleaks.toml, .grype.yaml, trivy.yaml, .semgrepignore), so local runs and
# CI cannot drift. High/critical findings fail; lower severities are reported.
#
# Usage:
#   bash dev/sast.sh                 # all scans except the image scan
#   bash dev/sast.sh semgrep         # a single tool
#   bash dev/sast.sh image           # docker build + grype/trivy + image SBOM
#   bash dev/sast.sh sbom deny       # any subset
#
# Targets: deny grype semgrep gitleaks hadolint sbom image all

set -u

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# Docker Desktop on Windows (Git Bash) must not rewrite container paths.
export MSYS_NO_PATHCONV=1

# Tool images: semgrep, gitleaks and trivy track :latest like the gateway's
# sast.ps1 and CI; grype, hadolint and syft are pinned.
SEMGREP_IMAGE='semgrep/semgrep:latest'
GRYPE_IMAGE='anchore/grype:v0.87.0'
HADOLINT_IMAGE='hadolint/hadolint:v2.12.0-alpine'
GITLEAKS_IMAGE='zricethezav/gitleaks:latest'
TRIVY_IMAGE='aquasec/trivy:latest'
SYFT_IMAGE='anchore/syft:v1.18.1'

# Semgrep registry rulesets - keep in sync with the semgrep job in security.yml.
SEMGREP_RULESETS=(p/rust p/secrets p/docker)

IMAGE_TAG='featherbit-operator:sast'

if [ "$#" -eq 0 ] || [ "$1" = "all" ]; then
  set -- deny grype semgrep gitleaks hadolint sbom
fi

VERSION="$(sed -n 's/^version = "\(.*\)"/\1/p' "$REPO_ROOT/Cargo.toml" | head -n1)"

RESULT_NAMES=()
RESULT_CODES=()

# run_scan NAME CMD [ARGS...] - runs one scan and records pass/fail.
run_scan() {
  local name="$1"
  shift
  printf '\n=== %s ===\n' "$name"
  "$@"
  local code=$?
  RESULT_NAMES+=("$name")
  RESULT_CODES+=("$code")
}

scan_deny() {
  if ! command -v cargo-deny >/dev/null 2>&1; then
    echo 'cargo-deny is not installed; run: cargo install cargo-deny --locked' >&2
    return 1
  fi
  cargo deny --manifest-path "$REPO_ROOT/Cargo.toml" check
}

scan_grype() {
  # Named volume caches the vulnerability DB between runs.
  docker run --rm -v "$REPO_ROOT:/src:ro" \
    -v featherbit-operator-grype-db:/db -e GRYPE_DB_CACHE_DIR=/db \
    "$GRYPE_IMAGE" dir:/src -c /src/.grype.yaml
}

scan_semgrep() {
  local args=()
  local ruleset
  for ruleset in "${SEMGREP_RULESETS[@]}"; do
    args+=(--config "$ruleset")
  done
  docker run --rm -v "$REPO_ROOT:/src:ro" -w /src "$SEMGREP_IMAGE" \
    semgrep scan "${args[@]}" --severity ERROR --error --metrics=off
}

scan_gitleaks() {
  # `dir` scans the working tree (not just git history), which also covers
  # files that are not committed yet.
  docker run --rm -v "$REPO_ROOT:/src:ro" "$GITLEAKS_IMAGE" \
    dir /src --config /src/.gitleaks.toml --no-banner --redact
}

scan_hadolint() {
  docker run --rm -v "$REPO_ROOT:/src:ro" -w /src "$HADOLINT_IMAGE" \
    hadolint --config .hadolint.yaml Dockerfile
}

scan_sbom() {
  if ! cargo cyclonedx --version >/dev/null 2>&1; then
    echo 'cargo-cyclonedx is not installed; run: cargo install cargo-cyclonedx --locked' >&2
    return 1
  fi
  mkdir -p "$REPO_ROOT/sast-out/sbom"
  (
    cd "$REPO_ROOT" &&
      cargo cyclonedx --format json --override-filename featherbit-operator.cdx --all &&
      mv featherbit-operator.cdx.json "sast-out/sbom/featherbit-operator-$VERSION.cdx.json"
  )
}

scan_image_build() {
  docker build -t "$IMAGE_TAG" "$REPO_ROOT"
}

scan_image_grype() {
  docker run --rm -v "$REPO_ROOT:/src:ro" \
    -v featherbit-operator-grype-db:/db -e GRYPE_DB_CACHE_DIR=/db \
    "$GRYPE_IMAGE" docker-archive:/src/sast-out/featherbit-operator-image.tar -c /src/.grype.yaml
}

scan_image_trivy() {
  docker run --rm -v "$REPO_ROOT:/src:ro" \
    -v featherbit-operator-trivy-cache:/root/.cache/trivy \
    "$TRIVY_IMAGE" image --config /src/trivy.yaml --ignorefile /src/.trivyignore.yaml \
    --exit-code 1 --input /src/sast-out/featherbit-operator-image.tar
}

scan_image_sbom() {
  mkdir -p "$REPO_ROOT/sast-out/sbom"
  docker run --rm -v "$REPO_ROOT:/src:ro" -v "$REPO_ROOT/sast-out/sbom:/out" \
    "$SYFT_IMAGE" scan docker-archive:/src/sast-out/featherbit-operator-image.tar \
    --source-name featherbit-operator-image --source-version "$VERSION" \
    -o "cyclonedx-json=/out/featherbit-operator-image-$VERSION.cdx.json"
}

for target in "$@"; do
  case "$target" in
    deny) run_scan 'cargo-deny' scan_deny ;;
    grype) run_scan 'grype (filesystem)' scan_grype ;;
    semgrep) run_scan 'semgrep' scan_semgrep ;;
    gitleaks) run_scan 'gitleaks' scan_gitleaks ;;
    hadolint) run_scan 'hadolint' scan_hadolint ;;
    sbom) run_scan 'sbom (crate)' scan_sbom ;;
    image)
      run_scan 'image build' scan_image_build
      if [ "${RESULT_CODES[${#RESULT_CODES[@]} - 1]}" -ne 0 ]; then
        continue
      fi
      # Export once, scan the archive - avoids handing scanners the socket.
      mkdir -p "$REPO_ROOT/sast-out"
      docker save "$IMAGE_TAG" -o "$REPO_ROOT/sast-out/featherbit-operator-image.tar"
      run_scan 'grype (image)' scan_image_grype
      run_scan 'trivy (image)' scan_image_trivy
      run_scan 'sbom (image)' scan_image_sbom
      ;;
    *)
      echo "Unknown target '$target'. Targets: deny grype semgrep gitleaks hadolint sbom image all" >&2
      exit 2
      ;;
  esac
done

printf '\n=== Summary ===\n'
failed=0
for i in "${!RESULT_NAMES[@]}"; do
  if [ "${RESULT_CODES[$i]}" -eq 0 ]; then
    printf '  PASS  %s\n' "${RESULT_NAMES[$i]}"
  else
    printf '  FAIL  %s\n' "${RESULT_NAMES[$i]}"
    failed=1
  fi
done
exit "$failed"
