#set page(numbering: "1")

#align(center, text(17pt)[*31corefs Specification*])

#align(center, [
  31core \
  #link("mailto:31core@tutanota.com") \
  Version: 1.0-dev
])

#set heading(numbering: "1.")

#outline()

= Introduction
31corefs is a modern, cross-platform filesystem. It support advanced features like subvolume management, snapshot and CoW.

Supported features:
- Subvolume
- Snapshot
- Copy on Write

= Data structure

== Super block
The super block records meta data describing the filesystem.

```c
struct super_block {
    uint8_t magic_header[4];
    uint8_t version;
    uint8_t uuid[16];
    uint8_t label[256];
    uint64_t total_blocks;
    uint64_t used_blocks;
    uint64_t real_used_blocks;
    uint64_t default_subvol;
    uint64_t subvol_mgr;
};
```

The `magic_header` is pre-defined as `[0x31, 0xc0, 0x8e, 0xf5]`.

The `version` is defined as `0x01`.

The `uuid` is recommend to use UUIDv4.

NOTE: The `label` is a regular C string that ends with NULL character.

== Inode
Inode records the meta of a file.

Each Inode takes 64 bytes, and its data structure is as follow.

```c
struct inode {
    uint16_t permission;
    uint16_t uid;
    uint16_t gid;
    uint64_t atime;
    uint64_t ctime;
    uint64_t mtime;
    uint16_t hlinks;
    uint64_t size;
    uint64_t btree_root;
};
```

*Definitions:*

#table(
    columns: (auto, auto),
    [*Field*], [*Description*],
    [permission], [POSIX permission],
    [uid], [UID of owner],
    [gid], [GID of owner],
    [atime], [Last access time (unit: sec)],
    [ctime], [Last change time (unit: sec)],
    [mtime], [Last modify time (unit: sec)],
    [hlinks], [Count of hard links],
    [size], [File size],
    [btree_root], [Root B-Tree node block of content management]
)

*Empty inode*

If `permission` in Inode is `0xffff`, then it is an empty Inode.

*ACLs*

- `ACL_DIRECTORY`: 0b100000000000000
- `ACL_SYMBOLLINK`: 0b010000000000000
- `ACL_FILE`: 0b001000000000000

== Subvolume
=== Subvolume entry

A subvolume entry takes 64 bytes to describe a subvolume.

```c
struct subvolume_entry {
    uint64_t id;
    uint64_t inode_tree_root;
    uint64_t inode_alloc_block;
    uint64_t root_inode;
};

```

=== Subvolume manager

Subvolume manager is a linked list.
```c
struct subvolume_manager {
    uint64_t next;
    uint64_t count;
    subvolume_entry entries[63];
};
```

== B-Tree
=== B-Tree entry 

31corefs defines a generic B-Tree that is uesd in data block management and inode group management.

Leaf node entry takes 24 bytes.
```c
struct btree_leaf_entry {
    uint64_t key;
    uint64_t value;
    uint64_t rc;
};
```

Internal node entry takes 16 bytes.
```c
struct btree_internal_entry {
    uint64_t key;
    uint64_t value;
};
```

=== B-Tree node

A leaf B-Tree node contains 170 leaf entries.

```c
struct btree_leaf_node {
    uint64_t entry_count;
    uint64_t rc;
    btree_internal_entry entries[170];
};
```

An internal B-Tree node contains 255 internal entries.

```c
struct btree_internal_node {
    uint64_t entry_count;
    uint64_t rc;
    uint8_t depth; // only root node has this field
    btree_internal_entry entries[255];
};
```
