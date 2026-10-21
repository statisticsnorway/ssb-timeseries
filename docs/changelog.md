# Changelog

## 0.9.0

- **Breaking:** The unused `Temporality.NONE` member is removed; only `AT` and `FROM_TO` remain.
  Its use cases are clear (constants without versioning, model parameters with versioning), but it is left out only because deviating from the time-dimension logic would require a substantial refactoring.
- **Breaking:** `SeriesType` is now a frozen dataclass; use the constants `NONE_AT`, `NONE_FROM_TO`, `AS_OF_AT` and `AS_OF_FROM_TO`, also re-exported from the package root.
  The string-keyword constructor, the six factory methods, and `SuperEnum` are removed.
  `Dataset(data_type=...)` still accepts a canonical string such as `"NONE_AT"` (case-insensitive), and persisted names and directory layout are unchanged.

```{changelog}
```
