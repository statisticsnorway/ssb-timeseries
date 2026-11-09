# ADR-0010: A taxonomy may be a graph, not only a tree

Status: Accepted

## Context

KLASS is the legacy source the taxonomy class was built for.
It is linked to GSIM and models classifications as strict trees.
Multiple and competing standards are handled through correspondence mappings, which are limited.
Entity relations outside that model have no representation in the library.
The implementation aims to support a source that extends KLASS with such relations, but that source is not yet defined; a file-based definition is the likely form.

## Decision

A taxonomy is modelled as a directed graph of entities and relations; a tree is one shape that graph may take.
The model is not bounded by KLASS, so a source may carry relations that KLASS does not model.
Loading is delegated to loader objects behind a `TaxonomyLoader` protocol, so a source supplies rows instead of reimplementing the class.
KLASS remains a supported source, and its classifications remain trees.

## Consequences

- The tree library `bigtree` is replaced by the graph library `networkx`; the structure is a `networkx.DiGraph`.
- KLASS taxonomies are unchanged: they are trees, and no source is forced to use the extra expressive power.
- Relations KLASS cannot express are representable once a source supplies them.
- No such source exists yet; the extended source is undefined, and a file-based definition is the likely first form.
- The loader output is a `code`/`parentCode` table with one parent per row, so a node with several parents is written as repeated rows.
- The accessors still present the graph as one rooted hierarchy, joining multiple roots under a synthetic `"0"` node; a non-tree has no public presentation yet.
- Adding a source is a loader and a branch in the constructor; the loader is not yet injectable.
- The documented structure typology (`structure_type`, `levels`) is still not implemented; the graph is the part of the promise that landed.
