# ADR-0012: An archive's convention is separate from its destination

Status: Accepted

## Context

ADR-0008 makes the archive a handler that owns the layout of what it writes.
Where an archive goes and how it is named vary independently: one destination may hold archives for several organisations, and one organisation's naming may be used on several destinations.
The earlier snapshot handler encoded Statistics Norway's naming in its code, so it was both a destination and a convention at once.

## Decision

The library's archive handler is constructed for a destination and takes the naming convention as a value.
The convention is selected per archive.
The library ships a neutral default and the Statistics Norway standard as one value among others.
A convention describes folder order, the file name as segments, the timestamp form, character folding, the data format, and how versions are numbered and recognised.

## Consequences

- Destination and convention vary independently, so the shipped handler does not grow with the number of naming standards.
- The default assumes no organisation, so the Statistics Norway standard is opt-in.
- A wrong convention is silent: files are written where it says, and lookup and version numbering follow the same pattern.
- Changing the convention of an archive that already holds data strands the existing files, because they no longer match the pattern used to find them.
- A new convention is added inside the package, not by configuration alone; there is no registration point yet.
- The standard's deviations are localised in its own value rather than spread through the archiving code.
- The split belongs to the shipped handler; a handler brought from elsewhere may still write in one organisation's layout, which the protocol permits.
