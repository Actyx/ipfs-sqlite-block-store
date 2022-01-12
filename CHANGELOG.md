# Changelog

This changelog was started after the 0.9 release.

## Release 0.10

- update to `rusqlite` version 0.26

0.10.1: BROKEN

0.10.2: reinstate behaviour of cleaning up unreferenced CIDs

0.10.3: remove some exponential runtime traps for pathologically linked DAGs

0.10.4: make it possible again to use standalone `temp_pin()` (i.e. remove broken foreign key constraint)

0.10.5: make GC logging less obnoxious

## Release 0.9

- use `unblock_notify` feature of `rusqlite` to allow concurrent transactions to the same DB
- add `addtional_connection` function to obtain connections for concurrent use within the same process
- make initial recomputation of storage size stats concurrent, since it needs to read all the blocks
- always provide context in error return values
- make GC fully concurrent, with locks taken only for brief periods — it is now reasonable to always run a full GC
