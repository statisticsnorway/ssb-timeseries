# ADR-0004: Configuration is an external, validated file with named presets

Status: Accepted

## Context

The configuration describes where a dataset's data and metadata live.
A caller should not hard-code paths or repository names into production code.
Dapla runs independent processes on GCP, so the same package has to work in several environments and share one setup where the processes share storage.
New code has to pick up what is already there instead of reimplementing it.

## Decision

The configuration lives in an external JSON file.
`TIMESERIES_CONFIG` names the file; without it, the default preset applies.
The file is validated when it is applied, so a malformed or partial configuration is rejected.
Named presets are complete configurations; selecting one replaces the default.

## Consequences

- Configuration is data, not code: a user changes the file without changing the library.
- The same package runs in several environments, and processes that share a storage location share one configuration.
- A configuration error is caught when the configuration is applied.
- Validation is shallow: top-level keys and types are checked, while nested items can still fail when used.
- A preset is all-or-nothing: it replaces the default rather than merging with it, so a variation starts from the preset and overrides what differs; saving writes the complete configuration.
- Presets ship with the package, so a deployment with different infrastructure can name a known preset without forking.
- The configuration is process-wide and applied at import, so one process cannot hold two configurations.
- Import fails when the environment variable names a file that does not exist; the library does not create a configuration file.
- Schema changes are configuration migrations, and potentially breaking.
