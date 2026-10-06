# NVMe passthrough reads for `VocabularyOnDisk`

## What it does

With the runtime parameter `vocabulary-nvme-passthrough=true`, the words file
of a `VocabularyOnDisk` can be an NVMe generic character device
(`/dev/ngXnY`). Batch lookups whose words are far apart are then read with
native NVMe Read commands submitted through `io_uring`
(`IORING_OP_URING_CMD`), which bypass the page cache, the file system and the
block layer. All other reads of the words file go through the block device of
the same namespace (`/dev/nvmeXnY`), i.e. through the page cache.

The feature is off by default. Without the parameter, no code path changes.

## Components

- `src/util/NvmePassthrough.h`: file offset to LBA translation, whole-block
  read planning (`planBlockReads`, runs of at most 128 KiB that merge gaps up
  to a configurable number of 512-byte blocks), the median gap of a batch
  (`medianGapBytes`), the block device name for a generic character device,
  the fail-closed capability probe (`NVME_IOCTL_ID` must report the configured
  namespace), and the preparation of the 128-byte SQE.
- `IoUringPolicy` (`src/util/IoUringManager.{h,cpp}`): with
  `nvmePassthrough::Options`, the ring is created with `IORING_SETUP_SQE128 |
  IORING_SETUP_CQE32` (the NVMe driver rejects `uring_cmd` on other rings). A
  read of a capable fd whose range is whole logical blocks becomes a native
  NVMe Read; every other read stays a plain read on the same ring. The
  completion of a passthrough command carries the NVMe status (0 on success)
  instead of a byte count.
- `VocabularyOnDisk`: opens the block device next to the character device
  (checks the namespace id on both and requires 512-byte logical blocks), and
  routes each batch of word reads by its locality (see below). `operator[]`
  and `scanAll` always read through the block device.

## Locality-adaptive routing

For each batch, the words that still have to be read (all words, or those
not served from the page cache with `vocabulary-iouring-page-cache-fast-path`)
are sorted by file offset, and the median gap between consecutive words is
computed.

- Median gap at most `vocabulary-nvme-max-buffered-median-gap` (default
  128 KiB, the default readahead window): the batch is read through the
  block device, one plain read per word, exactly as without passthrough. The
  kernel's readahead serves several words per device read, which passthrough
  cannot do.
- Larger median gap, or fewer than two words: the batch is read with
  passthrough as whole-block runs that merge gaps of at most
  `vocabulary-nvme-max-gap-blocks` blocks (default 32 = 16 KiB; a command
  covers at most 256 blocks = 128 KiB), then each word is copied from the
  staging buffer to its place in the result.

A regular words file gets the same routing and coalescing with plain reads
(its capability probe fails), which keeps the path testable without NVMe
hardware.

## Deployment

1. The namespace must use 512-byte logical blocks.
2. Write the words file of the on-disk vocabulary (the file that
   `VocabularyOnDisk::open` receives; its offsets are in the file with the
   suffix `.offsets`) to the raw namespace starting at LBA 0, e.g.
   `dd if=<words file> of=/dev/nvmeXnY bs=1M oflag=direct`, and replace the
   words file by a symbolic link to `/dev/ngXnY`. The `.offsets` file stays a
   regular file.
3. The server needs read access to both `/dev/ngXnY` and `/dev/nvmeXnY`
   (`root` only by default) and a kernel with NVMe `uring_cmd` support (5.19
   or later).
4. Start the server with `--set-runtime-parameter
   vocabulary-nvme-passthrough=true` and, if the namespace id is not 1,
   `--set-runtime-parameter vocabulary-nvme-namespace-id=<id>`.

A misconfiguration (a character device that is not an NVMe generic character
device, a namespace id mismatch, a block size other than 512, no `io_uring`)
fails when the index is loaded.

## Out of scope

- IOPoll completion polling and SQPoll.
- Registered (fixed) buffers for the passthrough commands.
- Passthrough for other files than the vocabulary words file; writes.
