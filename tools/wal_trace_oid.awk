#!/usr/bin/awk -f
# Usage:
#   wal_reader <wal_file> | awk -f wal_trace_oid.awk -v oid=123

BEGIN {
    FS = "[][]";
}

{
    for (i = 2; i <= NF; i += 2) {
        if ($i == "oid" && $(i + 1) == oid) {
            print $0;
            next;
        }
    }
}
