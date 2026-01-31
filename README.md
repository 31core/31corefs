# 31corefs

31corefs is a modern, cross-platform filesystem written in Rust. It support advanced features like subvolume management, snapshot and CoW.

## Features

* Copy on Write
* Sparse file
* Subvolume and snapshot
* POSIX ACLs
* Case-sentitive

## Source tree structure

|Directory|Description|
|---------|-----------|
|core     |Core library to access 31corefs, without any platform-related code.|
|doc      |Secification written in typst.|
|utils    |Utilities for managing 31corefs such as `mkfs`, `dump`, ...|

## Bugs & Reports

Please report a bug or share your ideas by email `31core@tutanota.com`.
