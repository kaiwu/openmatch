#!/usr/bin/awk -f
# Usage:
#   wal_reader <wal_file> | awk -f wal_match_by_maker.awk -v maker=123

BEGIN {
    FS = "[][]";
}

{
    for (i = 2; i <= NF; i += 2) {
        if ($i == "m" && $(i + 1) == maker) {
            print $0;
            next;
        }
    }
}
