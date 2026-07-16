#!/usr/bin/env bash
#
# Stage 1 full backfill runner (resumable — safe to stop and re-run).
#
# Launch detached in tmux:
#   tmux new-session -d -s jira-ingest 'mkdir -p data && bash ingestor/run_backfill.sh 2>&1 | tee -a data/ingest.log'
# Reattach:  tmux attach -t jira-ingest      Detach: Ctrl-b then d
#
set -euo pipefail

# Run from the project root regardless of where this is invoked from.
cd "$(dirname "$0")/.."

# Tesseract (image OCR) is installed via Homebrew.
export PATH="/opt/homebrew/bin:$PATH"

# Corporate network performs TLS interception. Build a CA bundle from the
# macOS keychain (the same root the OS/browser already trust) so Python
# verifies TLS properly instead of disabling verification.
CA_BUNDLE="$PWD/certs/corp-ca-bundle.pem"
mkdir -p certs
security find-certificate -a -p /Library/Keychains/System.keychain > "$CA_BUNDLE"
security find-certificate -a -p /System/Library/Keychains/SystemRootCertificates.keychain >> "$CA_BUNDLE"
export SSL_CERT_FILE="$CA_BUNDLE"

# Credentials from .env (JIRA_BASE_URL, JIRA_EMAIL, JIRA_API_TOKEN) — never inline.
if [[ ! -f .env ]]; then
  echo "ERROR: .env not found in $PWD" >&2
  exit 1
fi
set -a
# shellcheck disable=SC1091
source .env
set +a

echo "Starting Stage 1 backfill at $(date)  (resumes from data/checkpoint.json if present)"
exec ./.venv/bin/python -m ingestor.fetch
