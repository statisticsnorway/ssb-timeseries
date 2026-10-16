# Changelog

## Unreleased

- **Breaking:** `SeriesType` is now a frozen dataclass; use the constants `NONE_AT`, `NONE_FROM_TO`, `AS_OF_AT` and `AS_OF_FROM_TO`, also re-exported from the package root.
  The string-keyword constructor, the six factory methods, and `SuperEnum` are removed.
  `Dataset(data_type=...)` still accepts a canonical string such as `"NONE_AT"` (case-insensitive), and persisted names and directory layout are unchanged.

```{changelog}
```
