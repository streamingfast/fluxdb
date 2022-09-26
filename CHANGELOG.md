# Change log

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Removed

* **Deprecation** The `FluxDBHandler#SpeculativeWritesFetcher` method is deprecated, use `FluxDBHandler#SpeculativeWritesFetcherByNum` instead.

### Added

- Added `FluxDB#ReadSingletEntries` to query the storage engine returning all entries for a precise singlet.

- Added speculative writes fetching using a particular block hash.

- Added `FluxDBHandler#ReversibleBlock` to fetch an existing block from internal ForkDB (reversible segment of the chain).

### Fixed

- Fixed `ReadSingletEntries` returning only one result.

- Fixed `ReadSingletEntries` where a singlet entry at height 0 was not included.

- Fixed `ReadSingletEntries` ordering which was incorrect putting entries from speculative writes at the end instead of at the beginning.

- Fixed a bug where it was not possible to read a singlet entry at height 0.

- Fixed a bug when reading a single table row and it's present in the index, it was not picked up correctly.
