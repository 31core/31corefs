#set page(numbering: "1")

#align(center)[#text(17pt)[*31corefs Specification*]]

#align(center)[
  Author: 31core\
  Email: #link("mailto:31core@tutanota.com") \
  Version: 1.0-dev
]

#set heading(numbering: "1.")

#outline(depth: 1)

= Introduction
31corefs is a modern, cross-platform filesystem. It support advanced features like subvolume management, snapshot and CoW.

Supported features:
- Subvolume
- Snapshot
- Copy on Write

= Definitions
#table(columns: (auto, auto),
    [BLOCK_SIZE], [Unit size of blocks, currently supports 4096 block size.]
)

Subvolume statement:
#table(columns: (auto, auto),
    [*Constant*], [*Value*],
    [SUBVOLUME_STATE_ALLOCATED], [1],
    [SUBVOLUME_STATE_REMOVED], [2]
)

= Super block
The super block is the first block of the physical device, it records metadata describing the filesystem.

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

*Field explanation*

#table(
    columns: (auto, auto),
    [*Field*], [*Explanation*],
    [magic_header], [Pre-defined as `[0x31, 0xc0, 0x8e, 0xf5]`],
    [version], [`0x01` for version 1],
    [uuid], [Recommend to use UUIDv4],
    [label], [A regular C string that ends with NULL character]
)

= Block group
The whole filesystem is divided into several block groups, each block group is an independent block allocator. A block group includes a bitmap block and $8 times 4096$ data blocks. The bitmap is ahead of the the data blocks and is uesd to tracking allocation of the data blocks.


= B-Tree
== B-Tree entry 

31corefs defines a generic B-Tree that is used to mapping a unique 64 bit unsigned integer to another, with CoW support, which is uesd in data block management and inode group management.

Leaf node entry takes 24 bytes, with a reference counter (rc),
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

== B-Tree node

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
    uint8_t depth[8];
    uint64_t rc;
    btree_internal_entry entries[255];
};
```

A B-Tree node (both internal and leaf) is stored in a block, its `rc` value means how many times did the block referenced, clone step must be performed before modification when `rc` is greater than `0`.

The root node uses `depth[0]` to store tree height, otherwise set `0`.

= Inode
Inode records the metadata of a file.

Each inode takes 64 bytes, and its data structure is as follow.

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
    [acl], [POSIX ACL],
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

An an empty Inode always has `acl` valued `0xffff`.

*ACLs*

#table(
    columns: (4 * 7%, 4 * 9%),
    [File type (7 bits)], [Permission (9 bits)]
)

*File type*

- `ACL_RUGULAR_FILE`: `0x1`
- `ACL_DIRECTORY`: `0x2`
- `ACL_SYMBOLLINK`: `0x4`
- `ACL_CHAR`: `0x8`
- `ACL_BLOCK`: `0x10`

*Permission*

#table(
    columns: (auto, auto, auto, auto, auto, auto, auto, auto, auto),
    table.cell(colspan: 3)[Owner],
    table.cell(colspan: 3)[Group],
    table.cell(colspan: 3)[Other],
    [R], [W], [X], [R], [W], [X], [R], [W], [X],
)

== Inode group
31corefs store a group of inodes (called "inode group") in a block, a group contains 64 inodes

=== Inode index
Given inode group $g$ (indexing from `0`) and the $x$st (indexing from `0`) inodes in the group, the inode number $i$ should be:

$ i = 64 times g + x $

=== Inode group management
The map from inode group to block number is maintained by a B-Tree, and the B-Tree key is regarded the inode group number.

= Subvolume
A subvolume contains an independent Inode allocation B-Tree, recording block counts of Inode groups.

== Subvolume entry
A subvolume entry takes 128 bytes to describe a subvolume.

```c
struct subvolume_entry {
    uint64_t id;
    uint64_t inode_tree_root;
    uint64_t root_inode;
    uint64_t bitmap;
    uint64_t shared_bitmap;
    uint64_t igroup_bitmap;
    uint64_t used_blocks;
    uint64_t real_used_blocks;
    uint64_t creation_date;
    uint64_t snaps;
    uint64_t parent_subvol;
    uint8_t state;
};
```

== Subvolume manager
*Definition*
```c
struct subvolume_manager {
    uint64_t next;
    uint64_t count;
    subvolume_entry entries[63];
};
```
Subvolume manager is a linked list.

== Creation of subvolume
Subvolume creation operation follows the following steps:
- Allocate a subvolume entry from subvolume manager
- Initialize *igroup bitmap*, *block bitmap* and *shared block bitmap*
- Mark `subvolume_entry.state` as `SUBVOLUME_STATE_ALLOCATED`

== Removal of subvolume
Subvolume removal operation follows the following steps:
- Release blocks marked in the subvolume bitmap
- If `subvolume_entry.snaps` is 0
  - Remove subvolume entry from subvolume manager
- If `subvolume_entry.snaps` is not 0
  - Mark `subvolume_entry.state` as `SUBVOLUME_STATE_REMOVED`

== Linked bitmap
*Definition*
```c
struct igroup_bitmap {
    uint64_t next;
    uint64_t rc;
    uint8_t bitmap_data[BLOCK_SIZE - 16];
};
```

Subvolume mark an allocated block on the subvolume bitmap after allocated with the global allocator, and unmark an block when release it. This subvolume bitmap will be used when destroying a subvolume.

== Linked content table
*Definition*
```c
struct linked_content_table {
    uint64_t next;
    uint8_t data[BLOCK_SIZE - 8];
};
```
Linked content table is a typical linked table used to store simple content.
