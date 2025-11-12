#!/bin/bash
set -e

# Fix permissions for metabase-data directory if it exists
if [ -d "/metabase-data" ]; then
    chown -R metabase:metabase /metabase-data || true
    chmod -R 755 /metabase-data || true
fi

# Switch to metabase user and run Metabase
exec gosu metabase java -jar /home/metabase/metabase.jar "$@"

