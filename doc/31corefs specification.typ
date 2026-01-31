#set page(numbering: "1")
#set par(justify: true)
#set table(
  stroke: (x, y) => if y == 0 {
    (bottom: 1pt + black)
  } else {
    (bottom: 0.5pt + black)
  },
)
#set heading(numbering: "1.")

#align(center)[#text(17pt)[*31corefs specification*]]

#align(center)[
  #table(columns: 2,
  stroke: none,
  align: left,
  [Author:], [31core],
  [Email:], link("mailto:31core@tutanota.com"),
  [Version:], [1.0-dev],
)]

#outline(depth: 1)

= Introduction
31corefs is a modern, cross-platform filesystem. It supports advanced features like subvolume management, snapshot and Copy-on-Write.

Supported features:
- Copy-on-Write
- Sparse file
- Subvolume and snapshot
- POSIX ACLs
- Case-sentitive

= Definitions
== Global constants

#table(columns: 2,
  [*Constant*], [*Description*],
  [`BLOCK_SIZE`], [Unit size of blocks, currently supports 4096 block size.]
)

== Byte order
All multi-byte integer fields are stored in Big Endian byte order.

= Super block
The super block is the first block of the physical device, it records metadata describing the filesystem.

*Definition*
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
    uint64_t creation_time;
};
```

*Field explanation*

#table(
  columns: 2,
  [*Field*], [*Explanation*],
  [`magic_header`], [Pre-defined as `[0x31, 0xc0, 0x8e, 0xf5]`.],
  [`version`], [`0x01` for version 1.],
  [`uuid`], [Recommend to use UUIDv4.],
  [`label`], [A regular C string that ends with `NULL` character which can be in ASCII or UTF-8 charset.],
  [`total_blocks`], [Total number of blocks in the filesystem.],
  [`used_blocks`], [Number of blocks allocated.],
  [`real_used_blocks`], [Number of blocks actually used by data.],
  [`default_subvol`], [The default subvolume ID.],
  [`subvol_mgr`], [Block number of the subvolume manager block.],
  [`creation_time`], [Creation time of the filesystem (unit: nano sec).],
)

= Block allocator
== Block group
The whole filesystem is divided into several block groups, each block group is an independent block allocator. A block group includes a meta block, a bitmap block and $8 times "BLOCK_SIZE"$ data blocks. The meta block is the first block of a block group, it records allocation status of the block groups. And the bitmap is the second block of a block group and it is uesd to tracking allocation of the data blocks.

#figure(caption: [Structure of block group])[
#table(
  columns: 3,
  stroke: 0.5pt,
  [meta block], [bitmap block], [data blocks],
  [1 block], [1 block], [less than or equal to $8 times "BLOCK_SIZE"$ blocks],
)]

== Meta block
The meta block stores a structure that describes the state of its block group.

*Definition*
```c
struct block_group_meta {
    uint64_t id;
    uint64_t next_group;
    uint64_t capacity;
    uint64_t free_blocks;
};
```

#table(
  columns: 2,
  [*Field*], [*Description*],
  [`id`], [Block group ID, starting from `0`.],
  [`next_group`], [Physical block address of the next block group. A value of `0` marks the last group in the chain.],
  [`capacity`], [Total number of data blocks in the group.],
  [`free_blocks`], [Current count of unallocated data blocks in the group.],
)

*Note:* Block group loading *must* follows `next_group` in `block_group_meta`, never assume a fixed block number for the next group.

== Block allocation
Traverse block groups to find a block group where `block_group_meta.free_blocks` is greater than `0`, and then traverse bits in the bitmap block to find a free block. And then mark the bit to 1, decrease `block_group_meta.free_blocks` by 1, finally returns the block number.

= B-Tree
31corefs defines a generic B-Tree for mapping a unique 64 bit unsigned integer to another, with copy-on-write support, which is mainly uesd in data block management and inode group management.

== B-Tree entry
B-Tree entry is a key-value pair stored in B-Tree nodes. It has two types: leaf entry and internal entry.

Leaf node entry takes 20 bytes, with a reference counter (rc),
```c
struct btree_leaf_entry {
    uint64_t key;
    uint64_t value;
    uint32_t rc;
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
A leaf B-Tree node contains 204 leaf entries.

*Definition*
```c
struct btree_leaf_node {
    uint16_t entry_count;
    uint8_t reserved1;
    uint8_t type;
    uint32_t rc;
    uint64_t reserved2;
    struct btree_internal_entry entries[204];
};
```

An internal B-Tree node contains 255 internal entries.

```c
struct btree_internal_node {
    uint16_t entry_count;
    uint8_t reserved1;
    uint8_t type;
    uint32_t rc;
    uint64_t reserved2;
    struct btree_internal_entry entries[255];
};
```

*B-Tree type definitions*
#table(columns: 2,
  [*Constant*], [*Value*],
  [`BTREE_NODE_TYPE_INTERNAL`], [`0xf0`],
  [`BTREE_NODE_TYPE_LEAF`], [`0x0f`],
)

A B-Tree node (both internal and leaf) is stored in a block, its `rc` value means how many times did the block referenced, clone step must be performed before modification when `rc` is greater than `0` (see @btree-cow).

== B-Tree clone
Simply increase the `rc` value of the root B-Tree node by `1` when cloning a B-Tree.

== B-Tree copy-on-write <btree-cow>
When modifying a B-Tree node (insertion, deletion, or update), if the node's reference count (`rc`) is greater than `0`, perform a copy-on-write operation. This involves creating a new copy of the node, plusing its child nodes' `rc` to the node's `rc`, setting the original node's `rc` to `0`, and updating the parent node to point to the new copy. This ensures that other references to the original node remain unaffected by the modification.


== B-Tree insertion
To insert a key-value pair into a B-Tree, traverse from the root node to find the leaf node that should contain the key. If the leaf node has space, insert the entry directly. If the leaf node is full, split the node into two nodes and promote the middle key to the parent node. If the parent node is also full, repeat the splitting process up to the root. If the root node is split, create a new root node.

== B-Tree deletion
To delete a key-value pair from a B-Tree, traverse from the root node to find the leaf node that contains the key. Remove the entry from the leaf node. If the leaf node has entries less than $T - 1$ ($T$ refers to degree, 128 for internal node and 102 for leaf node), try to merge the leaf node with a sibling node. If merging is not possible, borrow an entry from a sibling node. If the parent node has entries less than $T - 1$, repeat the borrowing or merging process up to the root. If the root node has no entries, make its only child the new root.

= Inode & Inode group
== Inode 
Inode records the metadata of a file.

Each inode takes 64 bytes, and its data structure is as follow.

*Definition*
```c
struct inode {
    uint16_t type_acl;
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

*Field explanation*
#table(
  columns: 2,
  [*Field*], [*Description*],
  [`type_acl`], [Inode type and POSIX ACL],
  [`uid`], [UID of owner],
  [`gid`], [GID of owner],
  [`atime`], [Last access time (unit: nano sec)],
  [`ctime`], [Last change time (unit: nano sec)],
  [`mtime`], [Last modify time (unit: nano sec)],
  [`hlinks`], [Count of hard links],
  [`size`], [File size (unit: byte)],
  [`btree_root`], [Root B-Tree node block of content management]
)

*Empty inode*

An empty Inode always has `type_acl` valued `0xffff`.

*Type_ACL*

#table(
  columns: (4 * 7%, 4 * 9%),
  stroke: (x, y) => if y == 0 {
    if x == 0 {
      (top: 0.5pt, left: 0.5pt)
    } else {
      (top: 0.5pt, right: 0.5pt)
    }
  } else {
    (0.5pt)
  },
  [#sym.arrow.l High],
  table.cell(align: right)[Low #sym.arrow.r],
  [Type (7 bits)], [ACL (9 bits)]
)

*File type*

#table(columns: 2,
  [*Constant*], [*Value*],
  [`ITYPE_REGULAR_FILE`], [`0x1`],
  [`ITYPE_DIRECTORY`], [`0x2`],
  [`ITYPE_SYMBOLLINK`], [`0x4`],
  [`ITYPE_CHAR`], [`0x8`],
  [`ITYPE_BLOCK`], [`0x10`],
)

*ACL*

#table(
  columns: 9,
  stroke: (x, y) => if y == 0 {
    if x == 0 {
      (top: 0.5pt, left: 0.5pt)
    } else if x == 6 {
      (top: 0.5pt, right: 0.5pt)
    } else {
      (top: 0.5pt)
    }
  } else {
    (0.5pt)
  },
  /* Description of low and high bits */
  table.cell(colspan: 3)[#sym.arrow.l High],
  table.cell(colspan: 3)[],
  table.cell(colspan: 3, align: right)[Low #sym.arrow.r],
  table.cell(colspan: 3)[Owner],
  table.cell(colspan: 3)[Group],
  table.cell(colspan: 3)[Other],
  [R], [W], [X], [R], [W], [X], [R], [W], [X],
)

== Inode group
31corefs store a group of inodes (called "inode group") in a block, a group contains 64 inodes

=== Inode index
Given inode group $g$ (indexing from `0`) and the $x$st (indexing from `0`) inodes in the group, the absolute inode number $i$ could be calculated:

$ i = 64 times g + x $

=== Inode group management
The map from inode group to block number is maintained by a B-Tree, and the B-Tree key is regarded the inode group number.

= Subvolume
A subvolume contains an independent Inode allocation B-Tree, recording block counts of Inode groups.

== Subvolume entry
A subvolume entry takes 128 bytes to describe a subvolume.

*Definition*
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
    uint8_t flags;
};
```

Subvolume statement used by `state` field:
#table(columns: 2,
  [*Constant*], [*Value*],
  [`SUBVOLUME_STATE_ALLOCATED`], [`0x01`],
  [`SUBVOLUME_STATE_REMOVED`], [`0x02`]
)

Subvolume statement used by `flags` field:
#table(columns: 2,
  [*Constant*], [*Value*],
  [`SUBVOLUME_FLAG_READONLY`], [`0x01`],
)

== Subvolume manager
*Definition*
```c
struct subvolume_manager {
    uint64_t next;
    uint64_t count;
    struct subvolume_entry entries[63];
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

= Linked content table
*Definition*
```c
struct linked_content_table {
    uint64_t next;
    uint8_t content[BLOCK_SIZE - 8];
};
```
Linked content table is a typical linked table used to store simple content, such as symbol link.
