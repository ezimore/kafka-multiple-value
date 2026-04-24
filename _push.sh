#!/bin/bash
export GIT_PAGER=cat

git add -A
git commit -F - <<'MSG'
feat: SASL auth for perf benchmark, interactive producer improvements

- Benchmark Option 2 now uses SASL/PLAIN with individual users (alice,
  bob, carol) so the MLAPlugin can distinguish consumers and apply
  per-consumer bitmap filtering accurately.
- BenchmarkConsumer accepts optional saslUser/saslPassword args.
- SetupRegistry accepts optional principal names as arguments.
- Interactive producer: default message text "Hi", batch send support,
  authorized consumer names appended to message value.
- Fix duplicate produce calls in interactive producer.
MSG

echo "COMMIT_EXIT: $?"
git push origin record-fetch-plugin
echo "PUSH_EXIT: $?"
