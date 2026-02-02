#!/usr/bin/awk -f
# Usage:
#   wal_reader <wal_file> | awk -f wal_sum_qty_by_maker.awk

BEGIN {
    FS = "[][]";
}

{
    m = "";
    q = "";
    for (i = 2; i <= NF; i += 2) {
        if ($i == "m") {
            m = $(i + 1);
        } else if ($i == "q") {
            q = $(i + 1);
        }
    }
    if (m != "" && q != "") {
        sum[m] += q;
    }
}

END {
    for (id in sum) {
        print id, sum[id];
    }
}
