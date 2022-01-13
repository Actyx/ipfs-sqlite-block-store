# Changelog

This changelog was started sometime after the 0.7 release.

## Release 0.10

- update to `rusqlite` version 0.26

0.10.1: BROKEN

0.10.2: reinstate behaviour of cleaning up unreferenced CIDs

0.10.3: remove some exponential runtime traps for pathologically linked DAGs

0.10.4: make it possible again to use standalone `temp_pin()` (i.e. remove broken foreign key constraint)

0.10.5: make GC logging less obnoxious

0.10.6: fix CID cleanup to never remove temp_pins

0.10.7: fix GC to actually remove blocks from the refs table

## Release 0.9

- use `unblock_notify` feature of `rusqlite` to allow concurrent transactions to the same DB
- add `addtional_connection` function to obtain connections for concurrent use within the same process
- make initial recomputation of storage size stats concurrent, since it needs to read all the blocks
- always provide context in error return values
- make GC fully concurrent, with locks taken only for brief periods — it is now reasonable to always run a full GC

## Release 0.8

- no changes from 0.7 except for updating to `rusqlite` version 0.26 (which was reverted in 0.9; the proper update was only done in 0.10)
