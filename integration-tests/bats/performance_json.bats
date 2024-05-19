#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

# NOTE: These are currently disabled because the high variance in GitHub CI makes them unreliable.

# This BATS test attempts to detect performance regressions when using standard workflows on large datasets.
# Please note that this is a rough approach that is not designed to detect all performance issues, merely an extra
# safeguard against bugs that cause large (order-of-magnitude+) regressions.

# BATS_TEST_TIMEOUT is measured in seconds and is chosen to be high enough that all tests in this suite pass
# when running on GitHub's CI, but low enough that an order-of magnitude regression will cause them to fail.
BATS_TEST_TIMEOUT=200

# This function was used to create the dolt repo used for this test. It is not run during testing.
create_repo() {
    dolt init
    dolt checkout -b json

    dolt sql <<SQL
create table jsonTable (pk int primary key, j json);
insert into jsonTable (
  with recursive cte (pk, j) as (
    select 0, JSON_OBJECT("a", 1e1)
         union all
        select pk+1, JSON_INSERT(j, CONCAT("$.", pk), j) from cte where pk < 23
  ) select * from cte);
SQL

    dolt commit -Am "new table json"

    dolt branch
}

setup() {
    cp -r $BATS_TEST_DIRNAME/performance-repo/ $BATS_TMPDIR/dolt-repo-$$
    cd $BATS_TMPDIR/dolt-repo-$$
}

getTime() {
  cat "$1" | head -n 2 | tail -n 1 | cut -f 2
}

# SELECTing a json document should scale linearly with the size of the document.
@test "performance-json: SELECT *" {
    run dolt sql -q 'select * from jsonTable where pk = 2;'
    echo "$output"
    [ $status -eq 0 ]
    [[ "$output" =~ '| 2  | {"0":{"a":10},"1":{"0":{"a":10},"a":10},"a":10} |' ]] || false
    keys=$(dolt sql -q 'select pk from jsonTable where pk > 0;' -r csv | tail -n +2)
    for key in $keys; do
      { time dolt sql -q 'select * from jsonTable where pk = '"$key"';';} 2> log.txt
      echo "$key $(getTime log.txt)" >&3
    done
}


# JSON_VALUE should be as fast as a table point lookup.
# It can scale with the size of the returned value.
# It should not scale linearly with the document size.
@test "performance-json: JSON_VALUE" {
    keys=$(dolt sql -q 'select pk from jsonTable where pk > 0;' -r csv | tail -n +2)
    for key in $keys; do
      { time dolt sql -q 'select JSON_VALUE(j, "$.0") from jsonTable where pk = '"$key"';';} 2> log.txt
      echo "$key $(getTime log.txt)" >&3
    done
}

# JSON_INSERT should be as fast as a table point lookup.
# It can scale with the size of the inserted value.
# It should not scale linearly with the document size.
@test "performance-json: JSON_INSERT" {
    keys=$(dolt sql -q 'select pk from jsonTable;' -r csv | tail -n +2)
    for key in $keys; do
      { time dolt sql -q 'select JSON_INSERT(j, "$.x", 0) from jsonTable where pk = '"$key"';';} 2> log.txt
      echo "$key $(getTime log.txt)" >&3
    done
}

# JSON_REPLACE should be as fast as a table point lookup.
# It can scale with the size of the removed value.
# It should not scale linearly with the document size.
@test "performance-json: JSON_REPLACE" {
    keys=$(dolt sql -q 'select pk from jsonTable where pk > 0;' -r csv | tail -n +2)
    for key in $keys; do
      { time dolt sql -q 'update jsonTable set j = JSON_REPLACE(j, "$.0", 0) where pk = '"$key"';';} 2> log.txt
      echo "$key $(getTime log.txt)" >&3
    done
}

# JSON_REMOVE should be as fast as a table point lookup.
# It can scale with the size of the removed value.
# It should not scale linearly with the document size.
@test "performance-json: JSON_REMOVE" {
    keys=$(dolt sql -q 'select pk from jsonTable where pk > 0;' -r csv | tail -n +2)
    for key in $keys; do
      { time dolt sql -q 'update jsonTable set j = JSON_REMOVE(j, "$.0") where pk = '"$key"';';} 2> log.txt
      echo "$key $(getTime log.txt)" >&3
    done
}

# A three way merge should scale with the size of the diffs.
@test "performance-json: Three-way merge" {
    keys=$(dolt sql -q 'select pk from jsonTable where pk > 0;' -r csv | tail -n +2)
    for key in $keys; do
      dolt branch left
      dolt branch right
      dolt checkout left
      dolt sql -q 'update jsonTable set j = JSON_REMOVE(j, "$.0") where pk = '"$key"';'
      dolt add .
      dolt commit -m "modify left"
      dolt checkout right
      dolt sql -q 'update jsonTable set j = JSON_INSERT(j, "$.x", 1e1) where pk = '"$key"';'
      dolt add .
      dolt commit -m "modify right"
      { time dolt merge left;} 2> log.txt
      echo "$key $(getTime log.txt)" >&3
      dolt checkout json
      dolt branch -D left
      dolt branch -D right
    done
}