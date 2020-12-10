# IPFS sqlite block store &emsp; [![Latest Version]][crates.io] [![Docs Badge]][docs.rs]

[Latest Version]: https://img.shields.io/crates/v/ipfs-sqlite-block-store.svg
[crates.io]: https://crates.io/crates/ipfs-sqlite-block-store
[Docs Badge]: https://img.shields.io/badge/docs-docs.rs-green
[docs.rs]: https://docs.rs/ipfs-sqlite-block-store

<!-- cargo-sync-readme start -->

# IPFS sqlite block store

A block store for a rust implementation of [ipfs](https://ipfs.io/).

# Concepts

## Aliases

An alias is a named pin of a root. When a root is aliased, none of the leaves of the dag pointed
to by the root will be collected by gc. However, a root being aliased does not mean that the dag
must be complete.

## Temporary pins

A temporary pin is an unnamed pin that is just for the purpose of protecting blocks from gc
while a large tree is being constructed. While an alias maps a single name to a single root, a
temporary pin can be assigned to an arbitary number of blocks before the dag is finished.

A temporary pin will be deleted as soon as the handle goes out of scope.

## GC (Garbage Collection)

GC refers to the process of removing unpinned blocks. It runs only when the configured size
targets are exceeded. Size targets contain both the total size of the store and the number of
blocks.

GC will run incrementally, deleting blocks until the size targets are no longer exceeded. The
order in which unpinned blocks will be deleted can be customized.

## Caching

For unpinned blocks, it is possible to customize which blocks have the highest value. 

<!-- cargo-sync-readme end -->
