# AGENTS.md

This file provides repository-specific guidance to agents working in this repository.

## Testbed Hard Constraint

For any test, debug run, `test-plan`, or `tcctl` execution that requires a `testbed`, the following rules are mandatory:

1. Always create a new testbed for the current run. Do not reuse, attach to, or mutate an already existing testbed.
2. Use a unique testbed name for each run, for example with a timestamp or random suffix, so it is clearly scoped to the current execution.
3. Only delete the testbed created by the current run. Never delete a pre-existing or ambiguous-ownership testbed.
4. After the test finishes, delete the testbed created for that run before reporting completion. This applies to both success and failure paths.
5. If cleanup fails, do not silently ignore it. Report the exact testbed name, the cleanup command attempted, and the blocker.
6. If a script, workflow, or cached configuration defaults to reusing an existing testbed, override that behavior or stop and report that the workflow is incompatible with this rule.

This is a hard requirement and takes precedence over convenience, speed, or existing local habits.
