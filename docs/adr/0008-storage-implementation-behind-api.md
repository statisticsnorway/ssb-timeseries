# ADR-0008: The storage implementation is a technical detail behind the API

Status: Accepted

## Context

ADR-0003 made storage access independent of the backend, but kept the filesystem as the boundary and configuration as the source of dataset paths.
That holds while every repository is a filesystem, and it leaves operations a file interface cannot express out of reach.
The library is built around an information model: a repository holds datasets, and a dataset holds series (ADR-0001).
The information model is the stable part of the library; the storage technology is not, and production code should not be tied to the one the proof of concept used.
ADR-0007 makes the same separation for the in-memory representation.

## Decision

The API is defined over the information model; the storage implementation is a technical detail beneath it.
A handler maps a dataset reference onto a storage and owns the layout below that mapping, including where the version lives; the library constructs no dataset paths and parses no names.
The contract is a `typing.Protocol`, so compatibility is structural: a handler is compatible by satisfying the protocol, not by inheriting from a class of ours.
Each role has its own protocol: data, metadata and archive.
Handlers are registered in the configuration under a name and configured with their own options.
The direction is that no operation in the API depends on a storage technology, and that storage is reached through a handler; the shipped handlers are the current reach of that direction, not its limit.

## Consequences

- A new backend that fits the protocol is a module and configuration entries, not a change to the API or the library.
- Replacing a repository's storage is a data migration, because the layout belongs to the handler.
- No operation takes a path, a file name or a format; the model addresses repositories, datasets and series.
- The information model is the stable surface; storage technologies and their layouts are expected to change beneath it.
- Both shipped handlers are file-based, so the direction is proven across layouts rather than across storage technologies; a database or an object store is the test still outstanding.
- Protocol evolution is a breaking change for every handler outside the package.
- Handlers are resolved at runtime, so a bad name fails at first use rather than at import; a handler that does not satisfy its protocol is rejected with an error naming the missing operations.
- The runtime check tests which operations a handler exposes, not their signatures, so a handler whose signature does not match fails at the call.
- A repository is readable only through a handler that knows its layout; losing the handler loses the repository, though the data remains.
