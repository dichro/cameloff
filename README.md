# cameloff

This directory contains offline tools for Camlistore. They're likely
only useful if you have a diskpacked blobstore that is sufficiently
large that Camlistore's own index grows larger than your available
RAM.

## Indexing

To index an existing diskpacked blobstore:

`fsck scan --blob_dir /home/camlistore/blobs/ --db_dir /home/flash/fsck.db`

`fsck scan` can be killed and restarted freely; it will resume
scanning at the point that it left off. The directory named by
`--db_dir` will be automatically created if it doesn't exist.

## Missing Blobs

To find missing blobs, first complete a full `fsck scan` as above,
then run:

`fsck missing --blob_dir /home/camlistore/blobs/ --db_dir /home/flash/fsck.db`

`fsck` will display the known parents of each missing blob like so:

<pre>
sha1-005f3fb4a771f2db8bd07263dcd1061a09cf5a96
  + sha1-776e8ed57793f04408fa9ba6a34c3af8ab6737d9 (bytes)
    + sha1-7a04d7a5355ad6cb3ea91167099dceaa15a721dc (bytes)
      - sha1-7067b30571c6eeec5fd2db5b71c5316957134702 (file: "IMG0001.JPG")
    + sha1-c93c64d2f65c66984554db08cc6df9008a892743 (file: "img0001.jpg")
      + sha1-d70489bb214d9dd7689f8cebfb6428c9cc9123e2 (static-set)
        + sha1-c2aef3157f5966a0f14d2709a7d805258edd6a46 (directory: "dcim/img0001.jpg")
          + sha1-6816ec69bdf4a40ba635216c815e39a182ba7ec7 (static-set)
            - sha1-ef5fb6670a486b1ee72962aa4a515502ed825718 (directory: "sd/dcim/img0001.jpg")
</pre>

This example has a missing blob named
`sha1-005f3fb4a771f2db8bd07263dcd1061a09cf5a96` that is a member of
two distinct files, named `IMG0001.JPG` and `img0001.jpg`. The latter
is known to be under a directory hierarchy of `sd/dcim`.
