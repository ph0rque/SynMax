#!/usr/bin/env bash
set -euo pipefail

if [[ -f .env ]]; then
  # shellcheck disable=SC2046
  export $(grep -v '^#' .env | xargs)
fi

python -m agent.cli.main "$@"
