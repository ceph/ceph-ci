======================
Full RGW Object Dedup:
======================
Add a radosgw-admin command to collect and report deduplication stats

.. note:: This utility doesn’t perform dedup and doesn’t make any
          change to the existing system and will only collect
          statistics and report them.

----

***************
Admin commands:
***************
- ``radosgw-admin dedup estimate``
    Starts a new dedup estimate session (aborting first existing session if exists)
    It doesn't make any change to the existing system and will only collect statistics and report them.
- ``radosgw-admin dedup pause``:
   Pauses active dedup session (dedup resources are not released)
- ``radosgw-admin dedup resume``:
   Resumes a paused dedup session
- ``radosgw-admin dedup abort``:
   Aborts an active dedup session and release all resources used by it.
- ``radosgw-admin dedup stats``:
   Collects & displays last dedup statistics
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

----

********************
Estimate Processing:
********************
The Dedup Estimate process collects all the needed information directly from
the bucket indices reading one full bucket index object with thousands of
entries at a time.

The bucket indices objects are sharded between the participating
members so every bucket index object is read exactly one time.
The sharding allow processing to scale almost linearly splitting the
load evenly between the participating members.

The Dedup Estimate process does not access the objects themselves
(data/metadata) which means its processing time won't be affected by
the underlying media storing the objects (SSD/HDD) since the bucket indices are
virtually always stored on a fast medium (SSD with heavy memory
caching).

The admin can throttle the estimate process by setting a limit to the number of
bucket-index reads per-second per an RGW server (each read brings 1000 object entries) using:

$ radosgw-admin dedup throttle --max-bucket-index-ops=<count>

A typical RGW server performs about 100 bucket-index reads per second (i.e. 100,000 object entries).
Setting the count to 50 will typically slow down access by half and so on...

----

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
