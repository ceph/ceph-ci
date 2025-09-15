=====================
Full RGW Object Dedup
=====================
Adds ``radosgw-admin`` commands to remove duplicated RGW tail-objects and to collect and report deduplication stats.

**************
Admin commands
**************
- ``radosgw-admin dedup estimate``:
   Starts a new dedup estimate session (aborting first existing session if exists).
   It doesn't make any change to the existing system and will only collect statistics and report them.
- ``radosgw-admin dedup exec --yes-i-really-mean-it``:
   Starts a new dedup session (aborting first existing session if exists).
   It will perform a full dedup, finding duplicated tail-objects and removing them.

  This command can lead to **data-loss** and should not be used on production data!!
- ``radosgw-admin dedup pause``:
   Pauses active dedup session (dedup resources are not released)
- ``radosgw-admin dedup resume``:
   Resumes a paused dedup session
- ``radosgw-admin dedup abort``:
   Aborts an active dedup session and release all resources used by it.
- ``radosgw-admin dedup stats``:
   Collects & displays last dedup statistics.
- ``radosgw-admin dedup estimate``:
   Starts a new dedup estimate session (aborting first existing session if exists).
- ``radosgw-admin dedup throttle --max-bucket-index-ops=<count>``:
   Specify max bucket-index requests per second allowed for a single RGW server during dedup, 0 means unlimited.
- ``radosgw-admin dedup throttle --stat``:
   Display dedup throttle setting.

----

****************
Skipped Objects:
****************
Dedup Estimates skips the following objects:


- Objects smaller than 4MB (unless they are multipart)
- Objects with different placement rules
- Objects with different pools
- Objects with different same storage-classes

The Dedup process itself (which will be released later) will also skip
**compressed** and **user-encrypted** objects, but the estimate
process will accept them (since we don't have access to that
information during the estimate process)

The full dedup process skips all the above and it also skips **compressed** and **user-encrypted** objects.

----

********************
Estimate Processing:
********************
The Dedup Estimate process collects all the needed information directly from
the bucket-indices reading one full bucket-index object with 1000's of
entries at a time.

The Bucket-Indices objects are sharded between the participating
members so every bucket-index object is read exactly one time.
The sharding allow processing to scale almost linearly spliting the
load evenly between the participating members.

The Dedup Estimate process does not access the objects themselves
(data/metadata) which means its processing time won't be affected by
the underlined media storing the objects (SSD/HDD) since the bucket-indices are
virtually always stored on a fast medium (SSD with heavy memory
caching)


*************
Memory Usage:
*************
 +---------------++-----------+
 | RGW Obj Count |  Memory    |
 +===============++===========+
 | | ____1M      | | ___8MB   |
 | | ____4M      | | __16MB   |
 | | ___16M      | | __32MB   |
 | | ___64M      | | __64MB   |
 | | __256M      | | _128MB   |
 | | _1024M( 1G) | | _256MB   |
 | | _4096M( 4G) | | _512MB   |
 | | 16384M(16G) | | 1024MB   |
 +---------------+------------+

The admin can throttle the estimate process by setting a limit to the number of
bucket-index reads per-second per an RGW server (each read brings 1000 object entries) using:

$ radosgw-admin dedup throttle --max-bucket-index-ops=<count>

A typical RGW server performs about 100 bucket-index reads per second (i.e. 100,000 object entries).
Setting the count to 50 will typically slow down access by half and so on...

*********************
Full Dedup Processing
*********************
The Full Dedup process begins by constructing a dedup table from the bucket indices, similar to the estimate process above.

This table is then scanned linearly to purge objects without duplicates, leaving only dedup candidates.

Next, we iterate through these dedup candidate objects, reading their complete information from the object metadata (a per-object RADOS operation).
During this step, we filter out **compressed** and **user-encrypted** objects.

Following this, we calculate a strong-hash of the object data, which involves a full-object read and is a resource-intensive operation.
This strong-hash ensures that the dedup candidates are indeed perfect matches.
If they are, we proceed with the deduplication:

- incrementing the reference count on the source tail-objects one by one.
- copying the manifest from the source to the target.
- removing all tail-objects on the target.

************
Memory Usage
************
 +---------------+----------+
 | RGW Obj Count |  Memory  |
 +===============+==========+
 | 1M            | 8 MB     |
 +---------------+----------+
 | 4M            | 16 MB    |
 +---------------+----------+
 | 16M           | 32 MB    |
 +---------------+----------+
 | 64M           | 64 MB    |
 +---------------+----------+
 | 256M          | 128 MB   |
 +---------------+----------+
 | 1024M (1G)    | 256 MB   |
 +---------------+----------+
 | 4096M (4G)    | 512 MB   |
 +---------------+----------+
 | 16384M (16G)  | 1024 MB  |
 +---------------+----------+

