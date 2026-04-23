#!/bin/bash
export GIT_PAGER=cat

echo "=== Status ==="
git status --short | head -20

echo ""
echo "=== Staging all changes ==="
git add -A

echo ""
echo "=== Staged files ==="
git diff --cached --name-only | wc -l

echo ""
echo "=== Committing ==="
git commit -F - <<'MSG'
fix: offset advancement for filtered records, lazy plugin startup, manual testing tools

- Fix consumer offset stuck when all records in a fetch are filtered by
  RecordFetchPlugin: broker now writes a single advancement record with
  header record-fetch-plugin-filtered at the highest filtered offset so
  all consumer clients (Java, librdkafka) advance past filtered records.
- Fix MLAPlugin startup: resolve bootstrap servers from advertised.listeners
  or listeners config instead of requiring bootstrap.servers. Start registry
  client in background thread to avoid blocking broker startup.
- Add manual testing tools: mla_interactive_producer.py, mla_consumer.py,
  mla_register_consumers.py, mla_read_registry.py
- Add manual testing guide and performance benchmark suite
MSG

echo "COMMIT_EXIT: $?"

echo ""
echo "=== Pushing ==="
git push -u origin record-fetch-plugin
echo "PUSH_EXIT: $?"
