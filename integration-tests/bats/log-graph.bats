#!/usr/bin/env bats
load $BATS_TEST_DIRNAME/helper/common.bash

setup() {
    setup_common
}

teardown() {
    assert_feature_version
    teardown_common
}

@test "log-graph: basic --graph" {
    dolt commit --allow-empty -m "main 1"
    dolt checkout -b b1
    dolt commit --allow-empty -m "b1 1"

    dolt checkout main
    dolt merge --no-ff b1 -m "merge b1"

    run dolt log --graph
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 25 ]
    [[ $(echo "${lines[0]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "*   commit " ]] || false   # *   commit xxx
    [[ "${lines[1]}" =~ "|\  Merge: " ]] || false                                          # |\  Merge: xxx xxx
    [[ "${lines[2]}" =~ "| | Author: " ]] || false                                         # | | Author:
    [[ "${lines[3]}" =~ "| | Date: " ]] || false                                           # | | Date:
    [[ "${lines[4]}" =~ "| |" ]] || false                                                  # | |
    [[ "${lines[5]}" =~ "| | 	merge b1" ]] || false                                        # | | 	merge b1
    [[ "${lines[6]}" =~ "| |" ]] || false                                                  # | |
    [[ $(echo "${lines[7]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| * commit " ]] || false   # | * commit xxx
    [[ "${lines[8]}" =~ "|/  Author: " ]] || false                                         # |/  Author:
    [[ "${lines[9]}" =~ "|   Date: " ]] || false                                           # |   Date:
    [[ "${lines[10]}" =~ "|" ]] || false                                                   # |
    [[ "${lines[11]}" =~ "|   	b1 1" ]] || false                                          # |   	b1 1
    [[ "${lines[12]}" =~ "|" ]] || false                                                   # |
    [[ $(echo "${lines[13]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* commit " ]] || false    # * commit xxx
    [[ "${lines[14]}" =~ "| Author: " ]] || false                                          # | Author:
    [[ "${lines[15]}" =~ "| Date: " ]] || false                                            # | Date:
    [[ "${lines[16]}" =~ "|" ]] || false                                                   # |
    [[ "${lines[17]}" =~ "| 	main 1" ]] || false                                          # | 	main 1
    [[ "${lines[18]}" =~ "|" ]] || false                                                   # |
    [[ $(echo "${lines[19]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* commit " ]] || false    # * commit xxx
    [[ "${lines[20]}" =~ "| Author: " ]] || false                                          # | Author:
    [[ "${lines[21]}" =~ "| Date: " ]] || false                                            # | Date:
    [[ "${lines[22]}" =~ "|" ]] || false                                                   # |
    [[ "${lines[23]}" =~ "| 	Initialize data repository" ]] || false                      # | 	Initialize data repository
    [[ "${lines[24]}" =~ "|" ]] || false                                                   # |
}

@test "log-graph: multiple merges from one branch" {
    dolt commit --allow-empty -m "main 1"

    dolt checkout -b b1
    dolt commit --allow-empty -m "b1 1"
    dolt checkout main
    dolt commit --allow-empty -m "main 2"
    dolt merge b1 -m "merge b1"

    dolt checkout b1
    dolt commit --allow-empty -m "b1 2"
    dolt checkout main
    dolt merge b1 -m "merge b1 again"
    dolt commit --allow-empty -m "main 3"

    run dolt log --graph
    [ "$status" -eq 0 ]
    [ "${#lines[@]}" -eq 50 ]
    [[ $(echo "${lines[0]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* commit" ]] || false     # * commit xxx
    [[ "${lines[1]}" =~ "| Author:" ]] || false                                           # | Author:
    [[ "${lines[2]}" =~ "| Date:" ]] || false                                             # | Date:
    [[ "${lines[3]}" =~ "|" ]] || false                                                   # |
    [[ "${lines[4]}" =~ "| 	main 3" ]] || false                                           # |     main 3
    [[ "${lines[5]}" =~ "|" ]] || false                                                   # |
    [[ $(echo "${lines[6]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "*   commit" ]] || false   # *   commit xxx
    [[ "${lines[7]}" =~ "|\  Merge:" ]] || false                                          # |\  Merge: xxx xxx
    [[ "${lines[8]}" =~ "| | Author:" ]] || false                                         # | | Author:
    [[ "${lines[9]}" =~ "| | Date:" ]] || false                                           # | | Date:
    [[ "${lines[10]}" =~ "| |" ]] || false                                                # | |
    [[ "${lines[11]}" =~ "| | 	merge b1 again" ]] || false                               # | |     merge b1 again
    [[ "${lines[12]}" =~ "| |" ]] || false                                                # | |
    [[ $(echo "${lines[13]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| * commit" ]] || false  # | * commit xxx
    [[ "${lines[14]}" =~ "| | Author: " ]] || false                                       # | | Author:
    [[ "${lines[15]}" =~ "| | Date: " ]] || false                                         # | | Date:
    [[ "${lines[16]}" =~ "| |" ]] || false                                                # | |
    [[ "${lines[17]}" =~ "| | 	b1 2" ]] || false                                         # | |     b1 2
    [[ "${lines[18]}" =~ "| |" ]] || false                                                # | |
    [[ $(echo "${lines[19]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | commit " ]] || false # * | commit xxx
    [[ "${lines[20]}" =~ "|\| Merge: " ]] || false                                        # |\| Merge: xxx xxx
    [[ "${lines[21]}" =~ "| | Author: " ]] || false                                       # | | Author:
    [[ "${lines[22]}" =~ "| | Date: " ]] || false                                         # | | Date:
    [[ "${lines[23]}" =~ "| |" ]] || false                                                # | |
    [[ "${lines[24]}" =~ "| | 	merge b1" ]] || false                                     # | |     merge b1
    [[ "${lines[25]}" =~ "| |" ]] || false                                                # | |
    [[ $(echo "${lines[26]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | commit " ]] || false # * | commit xxx
    [[ "${lines[27]}" =~ "| | Author: " ]] || false                                       # | | Author:
    [[ "${lines[28]}" =~ "| | Date: " ]] || false                                         # | | Date:
    [[ "${lines[29]}" =~ "| |" ]] || false                                                # | |
    [[ "${lines[30]}" =~ "| | 	main 2" ]] || false                                       # | |     main 2
    [[ "${lines[31]}" =~ "| |" ]] || false                                                # | |
    [[ $(echo "${lines[32]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| * commit " ]] || false # | * commit xxx
    [[ "${lines[33]}" =~ "|/  Author: " ]] || false                                       # |/  Author:
    [[ "${lines[34]}" =~ "|   Date: " ]] || false                                         # |   Date:
    [[ "${lines[35]}" =~ "|" ]] || false                                                  # |
    [[ "${lines[36]}" =~ "|   	b1 1" ]] || false                                         # |     b1 1
    [[ "${lines[37]}" =~ "|" ]] || false                                                  # |
    [[ $(echo "${lines[38]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* commit " ]] || false   # * commit xxx
    [[ "${lines[39]}" =~ "| Author: " ]] || false                                         # | Author:
    [[ "${lines[40]}" =~ "| Date: " ]] || false                                           # | Date:
    [[ "${lines[41]}" =~ "|" ]] || false                                                  # |
    [[ "${lines[42]}" =~ "| 	main 1" ]] || false                                         # |     main 1
    [[ "${lines[43]}" =~ "|" ]] || false                                                  # |
    [[ $(echo "${lines[44]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* commit " ]] || false   # * commit xxx
    [[ "${lines[45]}" =~ "| Author: " ]] || false                                         # | Author:
    [[ "${lines[46]}" =~ "| Date: " ]] || false                                           # | Date:
    [[ "${lines[47]}" =~ "| " ]] || false                                                 # |
    [[ "${lines[48]}" =~ "| 	Initialize data repository" ]] || false                     # |     Initialize data repository
    [[ "${lines[49]}" =~ "|" ]] || false                                                  # |
}

@test "log-graph: merges from multiple branches" {
    dolt commit --allow-empty -m "main 1"

    dolt checkout -b b1
    dolt commit --allow-empty -m "b1 1"
    dolt checkout main
    dolt commit --allow-empty -m "main 2"
    dolt merge b1 -m "merge b1"

    dolt checkout -b b2
    dolt commit --allow-empty -m "b2 1"
    dolt checkout main
    dolt merge b2 --no-ff -m "merge b2"

    dolt checkout b1
    dolt commit --allow-empty -m "b1 2"
    dolt checkout main
    dolt merge b1 -m "merge b1 again"

    run dolt log --graph
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 57 ]
    [[ $(echo "${lines[0]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "*   commit " ]] || false    # *   commit xxx
    [[ "${lines[1]}" =~ "|\  Merge: " ]] || false                                           # |\  Merge: xxx xxx
    [[ "${lines[2]}" =~ "| | Author: " ]] || false                                          # | | Author:
    [[ "${lines[3]}" =~ "| | Date: " ]] || false                                            # | | Date:
    [[ "${lines[4]}" =~ "| |" ]] || false                                                   # | |
    [[ "${lines[5]}" =~ "| | 	merge b1 again" ]] || false                                   # | |     merge b1 again
    [[ "${lines[6]}" =~ "| |" ]] || false                                                   # | |
    [[ $(echo "${lines[7]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* |   commit " ]] || false  # * |   commit xxx
    [[ "${lines[8]}" =~ "|\ \  Merge: " ]] || false                                         # |\ \  Merge: xxx xxx
    [[ "${lines[9]}" =~ "| | | Author: " ]] || false                                        # | | | Author:
    [[ "${lines[10]}" =~ "| | | Date: " ]] || false                                         # | | | Date:
    [[ "${lines[11]}" =~ "| | |" ]] || false                                                # | | |
    [[ "${lines[12]}" =~ "| | | 	merge b2" ]] || false                                     # | | | 	merge b2
    [[ "${lines[13]}" =~ "| | |" ]] || false                                                # | | |
    [[ $(echo "${lines[14]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| * | commit " ]] || false # | * | commit xxx
    [[ "${lines[15]}" =~ "|/ /  Author: " ]] || false                                       # |/ /  Author: xxx xxx
    [[ "${lines[16]}" =~ "| |   Date: " ]] || false                                         # | |   Date:
    [[ "${lines[17]}" =~ "| |" ]] || false                                                  # | |
    [[ "${lines[18]}" =~ "| |   	b2 1" ]] || false                                         # | |   	b2 1
    [[ "${lines[19]}" =~ "| |" ]] || false                                                  # | |
    [[ $(echo "${lines[20]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| * commit " ]] || false   # | * commit xxx
    [[ "${lines[21]}" =~ "| | Author: " ]] || false                                         # | | Author:
    [[ "${lines[22]}" =~ "| | Date: " ]] || false                                           # | | Date:
    [[ "${lines[23]}" =~ "| |" ]] || false                                                  # | |
    [[ "${lines[24]}" =~ "| | 	b1 2" ]] || false                                           # | | 	b1 2
    [[ "${lines[25]}" =~ "| |" ]] || false                                                  # | |
    [[ $(echo "${lines[26]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | commit " ]] || false   # * | commit xxx
    [[ "${lines[27]}" =~ "|\| Merge: " ]] || false                                          # |\| Merge: xxx xxx
    [[ "${lines[28]}" =~ "| | Author: " ]] || false                                         # | | Author:
    [[ "${lines[29]}" =~ "| | Date: " ]] || false                                           # | | Date:
    [[ "${lines[30]}" =~ "| |" ]] || false                                                  # | |
    [[ "${lines[31]}" =~ "| | 	merge b1" ]] || false                                       # | | 	merge b1
    [[ "${lines[32]}" =~ "| |" ]] || false                                                  # | |
    [[ $(echo "${lines[33]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | commit " ]] || false   # * | commit xxx
    [[ "${lines[34]}" =~ "| | Author: " ]] || false                                         # | | Author:
    [[ "${lines[35]}" =~ "| | Date: " ]] || false                                           # | | Date:
    [[ "${lines[36]}" =~ "| |" ]] || false                                                  # | |
    [[ "${lines[37]}" =~ "| | 	main 2" ]] || false                                         # | | 	main 2
    [[ "${lines[38]}" =~ "| |" ]] || false                                                  # | |
    [[ $(echo "${lines[39]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| * commit " ]] || false   # | * commit xxx
    [[ "${lines[40]}" =~ "|/  Author: " ]] || false                                         # |/  Author:
    [[ "${lines[41]}" =~ "|   Date: " ]] || false                                           # |   Date:
    [[ "${lines[42]}" =~ "|" ]] || false                                                    # |
    [[ "${lines[43]}" =~ "|   	b1 1" ]] || false                                           # |     b1 1
    [[ "${lines[44]}" =~ "|" ]] || false                                                    # |
    [[ $(echo "${lines[45]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* commit " ]] || false     # * commit xxx
    [[ "${lines[46]}" =~ "| Author: " ]] || false                                           # | Author:
    [[ "${lines[47]}" =~ "| Date: " ]] || false                                             # | Date:
    [[ "${lines[48]}" =~ "|" ]] || false                                                    # |
    [[ "${lines[49]}" =~ "| 	main 1" ]] || false                                           # | 	main 1
    [[ "${lines[50]}" =~ "|" ]] || false                                                    # |
    [[ $(echo "${lines[51]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* commit " ]] || false     # * commit xxx
    [[ "${lines[52]}" =~ "| Author: " ]] || false                                           # | Author:
    [[ "${lines[53]}" =~ "| Date: " ]] || false                                             # | Date:
    [[ "${lines[54]}" =~ "| " ]] || false                                                   # |
    [[ "${lines[55]}" =~ "| 	Initialize data repository" ]] || false                       # | 	Initialize data repository
    [[ "${lines[56]}" =~ "| " ]] || false                                                   # |
}

@test "log-graph: merges with crossing edges" {
    dolt checkout -b b1
    dolt commit --allow-empty -m "b1 1"
    dolt checkout main
    dolt commit --allow-empty -m "main 1"

    dolt checkout -b b2
    dolt commit --allow-empty -m "b2 1"
    dolt checkout main
    dolt checkout -b b3
    dolt commit --allow-empty -m "b3 1"

    dolt checkout main
    dolt merge --no-ff b1 -m "merge b1"
    dolt merge --no-ff b2 -m "merge b2"
    dolt merge --no-ff b3 -m "merge b3"

    run dolt log --graph
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 51 ]
    [[ $(echo "${lines[0]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "*   commit " ]] || false         # *   commit xxx
    [[ "${lines[1]}" =~ "|\  Merge: " ]] || false                                                # |\  Merge: xxx xxx
    [[ "${lines[2]}" =~ "| | Author: " ]] || false                                               # | | Author:
    [[ "${lines[3]}" =~ "| | Date: " ]] || false                                                 # | | Date:
    [[ "${lines[4]}" =~ "| |" ]] || false                                                        # | |
    [[ "${lines[5]}" =~ "| | 	merge b3" ]] || false                                              # | | 	merge b3
    [[ "${lines[6]}" =~ "| |" ]] || false                                                        # | |
    [[ $(echo "${lines[7]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* |   commit " ]] || false       # * |   commit xxx
    [[ "${lines[8]}" =~ "|\ \  Merge: " ]] || false                                              # |\ \  Merge: xxx xxx
    [[ "${lines[9]}" =~ "| | | Author: " ]] || false                                             # | | | Author:
    [[ "${lines[10]}" =~ "| | | Date: " ]] || false                                              # | | | Date:
    [[ "${lines[11]}" =~ "| | |" ]] || false                                                     # | | |
    [[ "${lines[12]}" =~ "| | | 	merge b2" ]] || false                                          # | | | 	merge b2
    [[ "${lines[13]}" =~ "| | |" ]] || false                                                     # | | |
    [[ $(echo "${lines[14]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | |   commit " ]] || false    # * | |   commit xxx
    [[ "${lines[15]}" =~ "|\ \ \  Merge: " ]] || false                                           # |\ \ \  Merge: xxx xxx
    [[ "${lines[16]}" =~ "| | | | Author: " ]] || false                                          # | | | | Author:
    [[ "${lines[17]}" =~ "| | | | Date: " ]] || false                                            # | | | | Date:
    [[ "${lines[18]}" =~ "| | | |" ]] || false                                                   # | | | |
    [[ "${lines[19]}" =~ "| | | | 	merge b1" ]] || false                                        # | | | | 	merge b1
    [[ "${lines[20]}" =~ "| | | |" ]] || false                                                   # | | | |
    [[ $(echo "${lines[21]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | | * commit " ]] || false    # | | | * commit xxx
    [[ "${lines[22]}" =~ "| | |/  Author: " ]] || false                                          # | | |/  Author:
    [[ "${lines[23]}" =~ "| |/|   Date: " ]] || false                                            # | |/|   Date:
    [[ "${lines[24]}" =~ "|/| |" ]] || false                                                     # |/| |
    [[ "${lines[25]}" =~ "| | |   	b3 1" ]] || false                                            # | | |   	b3 1
    [[ "${lines[26]}" =~ "| | |" ]] || false                                                     # | | |
    [[ $(echo "${lines[27]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | * commit " ]] || false      # | | * commit
    [[ "${lines[28]}" =~ "| |/  Author: " ]] || false                                            # | |/  Author:
    [[ "${lines[29]}" =~ "|/|   Date: " ]] || false                                              # |/|   Date:
    [[ "${lines[30]}" =~ "| |" ]] || false                                                       # | |
    [[ "${lines[31]}" =~ "| |   	b2 1" ]] || false                                              # | |   	b2 1
    [[ "${lines[32]}" =~ "| |" ]] || false                                                       # | |
    [[ $(echo "${lines[33]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | commit " ]] || false        # * | commit
    [[ "${lines[34]}" =~ "| | Author: " ]] || false                                              # | | Author:
    [[ "${lines[35]}" =~ "| | Date: " ]] || false                                                # | | Date:
    [[ "${lines[36]}" =~ "| |" ]] || false                                                       # | |
    [[ "${lines[37]}" =~ "| | 	main 1" ]] || false                                              # | | 	main 1
    [[ "${lines[38]}" =~ "| |" ]] || false                                                       # | |
    [[ $(echo "${lines[39]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| * commit " ]] || false        # | * commit
    [[ "${lines[40]}" =~ "|/  Author: " ]] || false                                              # |/  Author:
    [[ "${lines[41]}" =~ "|   Date: " ]] || false                                                # |   Date:
    [[ "${lines[42]}" =~ "|" ]] || false                                                         # |
    [[ "${lines[43]}" =~ "|   	b1 1" ]] || false                                                # |   	b1 1
    [[ "${lines[44]}" =~ "|" ]] || false                                                         # |
    [[ $(echo "${lines[45]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* commit " ]] || false          # * commit
    [[ "${lines[46]}" =~ "| Author: " ]] || false                                                # | Author:
    [[ "${lines[47]}" =~ "| Date: " ]] || false                                                  # | Date:
    [[ "${lines[48]}" =~ "|" ]] || false                                                         # |
    [[ "${lines[49]}" =~ "| 	Initialize data repository" ]] || false                            # | 	Initialize data repository
    [[ "${lines[50]}" =~ "|" ]] || false                                                         # |
}

@test "log-graph: multiple merges with same parent" {
    dolt branch b2
    dolt branch b3
    dolt checkout -b b1
    dolt commit --allow-empty -m "b1 1"
    dolt checkout main
    dolt commit --allow-empty -m "main 1"

    dolt checkout main
    dolt checkout b2
    dolt commit --allow-empty -m "b2 1"
    dolt checkout main
    dolt checkout b3
    dolt commit --allow-empty -m "b3 1"

    dolt checkout main
    dolt merge b1 -m "merge b1"
    dolt merge b2 -m "merge b2"
    dolt merge b3 -m "merge b3"

    run dolt log --graph
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 51 ]
    [[ $(echo "${lines[0]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "*   commit " ]] || false         # *   commit xxx
    [[ "${lines[1]}" =~ "|\  Merge: " ]] || false                                                # |\  Merge: xxx xxx
    [[ "${lines[2]}" =~ "| | Author: " ]] || false                                               # | | Author:
    [[ "${lines[3]}" =~ "| | Date: " ]] || false                                                 # | | Date:
    [[ "${lines[4]}" =~ "| |" ]] || false                                                        # | |
    [[ "${lines[5]}" =~ "| | 	merge b3" ]] || false                                              # | | 	merge b3
    [[ "${lines[6]}" =~ "| |" ]] || false                                                        # | |
    [[ $(echo "${lines[7]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* |   commit " ]] || false       # * |   commit xxx
    [[ "${lines[8]}" =~ "|\ \  Merge: " ]] || false                                              # |\ \  Merge: xxx xxx
    [[ "${lines[9]}" =~ "| | | Author: " ]] || false                                             # | | | Author:
    [[ "${lines[10]}" =~ "| | | Date: " ]] || false                                              # | | | Date:
    [[ "${lines[11]}" =~ "| | |" ]] || false                                                     # | | |
    [[ "${lines[12]}" =~ "| | | 	merge b2" ]] || false                                          # | | | 	merge b2
    [[ "${lines[13]}" =~ "| | |" ]] || false                                                     # | | |
    [[ $(echo "${lines[14]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | |   commit " ]] || false    # * | |   commit xxx
    [[ "${lines[15]}" =~ "|\ \ \  Merge: " ]] || false                                           # |\ \ \  Merge: xxx xxx
    [[ "${lines[16]}" =~ "| | | | Author: " ]] || false                                          # | | | | Author:
    [[ "${lines[17]}" =~ "| | | | Date: " ]] || false                                            # | | | | Date:
    [[ "${lines[18]}" =~ "| | | |" ]] || false                                                   # | | | |
    [[ "${lines[19]}" =~ "| | | | 	merge b1" ]] || false                                        # | | | | 	merge b1
    [[ "${lines[20]}" =~ "| | | |" ]] || false                                                   # | | | |
    [[ $(echo "${lines[21]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | | * commit " ]] || false    # | | | * commit xxx
    [[ "${lines[22]}" =~ "| | | | Author: " ]] || false                                          # | | | | Author:
    [[ "${lines[23]}" =~ "| | | | Date: " ]] || false                                            # | | | | Date:
    [[ "${lines[24]}" =~ "| | | |" ]] || false                                                   # | | | |
    [[ "${lines[25]}" =~ "| | | | 	b3 1" ]] || false                                            # | | | | 	b3 1
    [[ "${lines[26]}" =~ "| | | |" ]] || false                                                   # | | | |
    [[ $(echo "${lines[27]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | * | commit " ]] || false    # | | * | commit xxx
    [[ "${lines[28]}" =~ "| | |/  Author: " ]] || false                                          # | | |/  Author:
    [[ "${lines[29]}" =~ "| | |   Date: " ]] || false                                            # | | |   Date:
    [[ "${lines[30]}" =~ "| | |" ]] || false                                                     # | | |
    [[ "${lines[31]}" =~ "| | |   	b2 1" ]] || false                                            # | | |   	b2 1
    [[ "${lines[32]}" =~ "| | |" ]] || false                                                     # | | |
    [[ $(echo "${lines[33]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | | commit " ]] || false      # * | | commit xxx
    [[ "${lines[34]}" =~ "| |/  Author: " ]] || false                                            # | |/  Author:
    [[ "${lines[35]}" =~ "|/|   Date: " ]] || false                                              # |/|   Date:
    [[ "${lines[36]}" =~ "| |" ]] || false                                                       # | |
    [[ "${lines[37]}" =~ "| |   	main 1" ]] || false                                            # | |   	main 1
    [[ "${lines[38]}" =~ "| |" ]] || false                                                       # | |
    [[ $(echo "${lines[39]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| * commit " ]] || false        # | * commit
    [[ "${lines[40]}" =~ "|/  Author: " ]] || false                                              # |/  Author:
    [[ "${lines[41]}" =~ "|   Date: " ]] || false                                                # |   Date:
    [[ "${lines[42]}" =~ "|" ]] || false                                                         # |
    [[ "${lines[43]}" =~ "|   	b1 1" ]] || false                                                # |   	b1 1
    [[ "${lines[44]}" =~ "|" ]] || false                                                         # |
    [[ $(echo "${lines[45]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* commit " ]] || false          # * commit
    [[ "${lines[46]}" =~ "| Author: " ]] || false                                                # | Author:
    [[ "${lines[47]}" =~ "| Date: " ]] || false                                                  # | Date:
    [[ "${lines[48]}" =~ "|" ]] || false                                                         # |
    [[ "${lines[49]}" =~ "| 	Initialize data repository" ]] || false                            # | 	Initialize data repository
    [[ "${lines[50]}" =~ "|" ]] || false                                                         # |
}

@test "log-graph: diagonal edge extends past commit message (outer edge)" {
    dolt checkout -b b1
    dolt commit --allow-empty -m "b1 1"
    dolt checkout main
    dolt checkout -b b2
    dolt commit --allow-empty -m "b2 1"
    dolt checkout main
    dolt checkout -b b3
    dolt commit --allow-empty -m "b3 1"
    dolt checkout main
    dolt checkout -b b4
    dolt commit --allow-empty -m "b4 1"
    dolt checkout main
    dolt checkout -b b5
    dolt commit --allow-empty -m "b5 1"
    dolt checkout main
    dolt commit --allow-empty -m "main 1"
    dolt checkout -b b6
    dolt commit --allow-empty -m "b6 1"

    dolt checkout main
    dolt merge b1 -m "merge b1"
    dolt merge b2 -m "merge b2"
    dolt merge b3 -m "merge b3"
    dolt merge b4 -m "merge b4"
    dolt merge b5 -m "merge b5"
    dolt merge b6 -m "merge b6"

    run dolt log --graph
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 91 ]
    [[ $(echo "${lines[0]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "*   commit " ]] || false             # *   commit xxx
    [[ "${lines[1]}" =~ "|\  Merge: " ]] || false                                                    # |\  Merge: xxx xxx
    [[ "${lines[2]}" =~ "| | Author: " ]] || false                                                   # | | Author:
    [[ "${lines[3]}" =~ "| | Date: " ]] || false                                                     # | | Date:
    [[ "${lines[4]}" =~ "| |" ]] || false                                                            # | |
    [[ "${lines[5]}" =~ "| | 	merge b6" ]] || false                                                  # | | 	merge b6
    [[ "${lines[6]}" =~ "| |" ]] || false                                                            # | |
    [[ $(echo "${lines[7]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* |   commit " ]] || false           # * |   commit xxx
    [[ "${lines[8]}" =~ "|\ \  Merge: " ]] || false                                                  # |\ \  Merge: xxx xxx
    [[ "${lines[9]}" =~ "| | | Author: " ]] || false                                                 # | | | Author:
    [[ "${lines[10]}" =~ "| | | Date: " ]] || false                                                  # | | | Date:
    [[ "${lines[11]}" =~ "| | |" ]] || false                                                         # | | |
    [[ "${lines[12]}" =~ "| | | 	merge b5" ]] || false                                              # | | | 	merge b5
    [[ "${lines[13]}" =~ "| | |" ]] || false                                                         # | | |
    [[ $(echo "${lines[14]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | |   commit " ]] || false        # * | |   commit xxx
    [[ "${lines[15]}" =~ "|\ \ \  Merge: " ]] || false                                               # |\ \ \  Merge: xxx xxx
    [[ "${lines[16]}" =~ "| | | | Author: " ]] || false                                              # | | | | Author
    [[ "${lines[17]}" =~ "| | | | Date: " ]] || false                                                # | | | | Date:
    [[ "${lines[18]}" =~ "| | | |" ]] || false                                                       # | | | |
    [[ "${lines[19]}" =~ "| | | | 	merge b4" ]] || false                                            # | | | | 	merge b4
    [[ "${lines[20]}" =~ "| | | |" ]] || false                                                       # | | | |
    [[ $(echo "${lines[21]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | | |   commit " ]] || false      # * | | |   commit xxx
    [[ "${lines[22]}" =~ "|\ \ \ \  Merge: " ]] || false                                             # |\ \ \ \  Merge: xxx xxx
    [[ "${lines[23]}" =~ "| | | | | Author: " ]] || false                                            # | | | | | Author:
    [[ "${lines[24]}" =~ "| | | | | Date: " ]] || false                                              # | | | | | Date:
    [[ "${lines[25]}" =~ "| | | | |" ]] || false                                                     # | | | | |
    [[ "${lines[26]}" =~ "| | | | | 	merge b3" ]] || false                                          # | | | | | 	merge b3
    [[ "${lines[27]}" =~ "| | | | |" ]] || false                                                     # | | | | |
    [[ $(echo "${lines[28]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | | | |   commit " ]] || false    # * | | | |   commit xxx
    [[ "${lines[29]}" =~ "|\ \ \ \ \  Merge: " ]] || false                                           # |\ \ \ \ \  Merge: xxx xxx
    [[ "${lines[30]}" =~ "| | | | | | Author: " ]] || false                                          # | | | | | | Author:
    [[ "${lines[31]}" =~ "| | | | | | Date: " ]] || false                                            # | | | | | | Date:
    [[ "${lines[32]}" =~ "| | | | | |" ]] || false                                                   # | | | | | |
    [[ "${lines[33]}" =~ "| | | | | | 	merge b2" ]] || false                                        # | | | | | | 	merge b2
    [[ "${lines[34]}" =~ "| | | | | |" ]] || false                                                   # | | | | | |
    [[ $(echo "${lines[35]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | | | | |   commit " ]] || false  # * | | | | |   commit xxx
    [[ "${lines[36]}" =~ "|\ \ \ \ \ \  Merge: " ]] || false                                         # |\ \ \ \ \ \  Merge: xxx xxx
    [[ "${lines[37]}" =~ "| | | | | | | Author: " ]] || false                                        # | | | | | | | Author:
    [[ "${lines[38]}" =~ "| | | | | | | Date: " ]] || false                                          # | | | | | | | Date:
    [[ "${lines[39]}" =~ "| | | | | | |" ]] || false                                                 # | | | | | | |
    [[ "${lines[40]}" =~ "| | | | | | | 	merge b1" ]] || false                                      # | | | | | | | 	merge b1
    [[ "${lines[41]}" =~ "| | | | | | |" ]] || false                                                 # | | | | | | |
    [[ $(echo "${lines[42]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | | | | | * commit " ]] || false  # | | | | | | * commit xxx
    [[ "${lines[43]}" =~ "| | | | | |/  Author: " ]] || false                                        # | | | | | |/  Author:
    [[ "${lines[44]}" =~ "| | | | |/|   Date: " ]] || false                                          # | | | | |/|   Date:
    [[ "${lines[45]}" =~ "| | | |/| |" ]] || false                                                   # | | | |/| |
    [[ "${lines[46]}" =~ "| | |/| | |   	b6 1" ]] || false                                          # | | |/| | |   	b6 1
    [[ "${lines[47]}" =~ "| |/| | | |" ]] || false                                                   # | |/| | | |
    [[ "${lines[48]}" =~ "|/| | | | |" ]] || false                                                   # |/| | | | |
    [[ $(echo "${lines[49]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | | | | | commit " ]] || false    # * | | | | | commit xxx
    [[ "${lines[50]}" =~ "| | | | | | Author: " ]] || false                                          # | | | | | | Author:
    [[ "${lines[51]}" =~ "| | | | | | Date: " ]] || false                                            # | | | | | | Date:
    [[ "${lines[52]}" =~ "| | | | | |" ]] || false                                                   # | | | | | |
    [[ "${lines[53]}" =~ "| | | | | | 	main 1" ]] || false                                          # | | | | | | 	main 1
    [[ "${lines[54]}" =~ "| | | | | |" ]] || false                                                       # | | | | | |
    [[ $(echo "${lines[55]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | | | | * commit " ]] || false    # | | | | | * commit xxx
    [[ "${lines[56]}" =~ "| | | | |/  Author: " ]] || false                                          # | | | | |/  Author:
    [[ "${lines[57]}" =~ "| | | |/|   Date: " ]] || false                                            # | | | |/|   Date:
    [[ "${lines[58]}" =~ "| | |/| |" ]] || false                                                     # | | |/| |
    [[ "${lines[59]}" =~ "| |/| | |   	b5 1" ]] || false                                            # | |/| | |   	b5 1
    [[ "${lines[60]}" =~ "|/| | | |" ]] || false                                                     # |/| | | |
    [[ $(echo "${lines[61]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | | | * commit " ]] || false      # | | | | * commit xxx
    [[ "${lines[62]}" =~ "| | | |/  Author: " ]] || false                                            # | | | |/  Author:
    [[ "${lines[63]}" =~ "| | |/|   Date: " ]] || false                                              # | | |/|   Date:
    [[ "${lines[64]}" =~ "| |/| |" ]] || false                                                       # | |/| |
    [[ "${lines[65]}" =~ "|/| | |   	b4 1" ]] || false                                              # |/| | |   	b4 1
    [[ "${lines[66]}" =~ "| | | |" ]] || false                                                       # | | | |
    [[ $(echo "${lines[67]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | | * commit " ]] || false        # | | | * commit xxx
    [[ "${lines[68]}" =~ "| | |/  Author: " ]] || false                                              # | | |/  Author:
    [[ "${lines[69]}" =~ "| |/|   Date: " ]] || false                                                # | |/|   Date:
    [[ "${lines[70]}" =~ "|/| |" ]] || false                                                         # |/| |
    [[ "${lines[71]}" =~ "| | |   	b3 1" ]] || false                                                # | | |   	b3 1
    [[ "${lines[72]}" =~ "| | |" ]] || false                                                         # | | |
    [[ $(echo "${lines[73]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | * commit " ]] || false          # | | * commit
    [[ "${lines[74]}" =~ "| |/  Author: " ]] || false                                                # | |/  Author:
    [[ "${lines[75]}" =~ "|/|   Date: " ]] || false                                                  # |/|   Date:
    [[ "${lines[76]}" =~ "| |" ]] || false                                                           # | |
    [[ "${lines[77]}" =~ "| |   	b2 1" ]] || false                                                  # | |   	b2 1
    [[ "${lines[78]}" =~ "| |" ]] || false                                                           # | |
    [[ $(echo "${lines[79]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| * commit " ]] || false            # | * commit xxx
    [[ "${lines[80]}" =~ "|/  Author: " ]] || false                                                  # |/  Author:
    [[ "${lines[81]}" =~ "|   Date: " ]] || false                                                    # |   Date:
    [[ "${lines[82]}" =~ "|" ]] || false                                                             # |
    [[ "${lines[83]}" =~ "|   	b1 1" ]] || false                                                    # |   	b1 1
    [[ "${lines[84]}" =~ "|" ]] || false                                                             # |
    [[ $(echo "${lines[85]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* commit " ]] || false              # * commit
    [[ "${lines[86]}" =~ "| Author: " ]] || false                                                    # | Author:
    [[ "${lines[87]}" =~ "| Date: " ]] || false                                                      # | Date:
    [[ "${lines[88]}" =~ "|" ]] || false                                                             # |
    [[ "${lines[89]}" =~ "| 	Initialize data repository" ]] || false                                # | 	Initialize data repository
    [[ "${lines[90]}" =~ "|" ]] || false                                                             # |
}

@test "log-graph: diagonal edge extends past commit message (not outer edge)" {
    dolt checkout -b b1
    dolt commit --allow-empty -m "b1 1"
    dolt checkout main
    dolt checkout -b b2
    dolt commit --allow-empty -m "b2 1"
    dolt checkout main
    dolt checkout -b b3
    dolt commit --allow-empty -m "b3 1"
    dolt checkout main
    dolt checkout -b b4
    dolt commit --allow-empty -m "b4 1"
    dolt checkout main
    dolt checkout -b b5
    dolt commit --allow-empty -m "b5 1"
    dolt checkout main
    dolt checkout -b b6
    dolt commit --allow-empty -m "b6 1"
    dolt checkout main
    dolt commit --allow-empty -m "main 1"
    dolt checkout -b b7
    dolt commit --allow-empty -m "b7 1"

    dolt checkout main
    dolt merge b1 -m "merge b1"
    dolt merge b2 -m "merge b2"
    dolt merge b3 -m "merge b3"
    dolt merge b4 -m "merge b4"
    dolt merge b5 -m "merge b5"
    dolt merge b7 -m "merge b7"
    dolt merge b6 -m "merge b6"

    run dolt log --graph
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 105 ]
    [[ $(echo "${lines[0]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "*   commit " ]] || false               # *   commit xxx
    [[ "${lines[1]}" =~ "|\  Merge: " ]] || false                                                      # |\  Merge: xxx xxx
    [[ "${lines[2]}" =~ "| | Author: " ]] || false                                                     # | | Author:
    [[ "${lines[3]}" =~ "| | Date: " ]] || false                                                       # | | Date:
    [[ "${lines[4]}" =~ "| |" ]] || false                                                              # | |
    [[ "${lines[5]}" =~ "| | 	merge b6" ]] || false                                                    # | | 	merge b6
    [[ "${lines[6]}" =~ "| |" ]] || false                                                              # | |
    [[ $(echo "${lines[7]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* |   commit " ]] || false             # * |   commit xxx
    [[ "${lines[8]}" =~ "|\ \  Merge: " ]] || false                                                    # |\ \  Merge: xxx xxx
    [[ "${lines[9]}" =~ "| | | Author: " ]] || false                                                   # | | | Author:
    [[ "${lines[10]}" =~ "| | | Date: " ]] || false                                                    # | | | Date:
    [[ "${lines[11]}" =~ "| | |" ]] || false                                                           # | | |
    [[ "${lines[12]}" =~ "| | | 	merge b7" ]] || false                                                # | | | 	merge b7
    [[ "${lines[13]}" =~ "| | |" ]] || false                                                           # | | |
    [[ $(echo "${lines[14]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | |   commit " ]] || false          # * | |   commit xxx
    [[ "${lines[15]}" =~ "|\ \ \  Merge: " ]] || false                                                 # |\ \ \  Merge: xxx xxx
    [[ "${lines[16]}" =~ "| | | | Author: " ]] || false                                                # | | | | Author
    [[ "${lines[17]}" =~ "| | | | Date: " ]] || false                                                  # | | | | Date:
    [[ "${lines[18]}" =~ "| | | |" ]] || false                                                         # | | | |
    [[ "${lines[19]}" =~ "| | | | 	merge b5" ]] || false                                              # | | | | 	merge b5
    [[ "${lines[20]}" =~ "| | | |" ]] || false                                                         # | | | |
    [[ $(echo "${lines[21]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | | |   commit " ]] || false        # * | | |   commit xxx
    [[ "${lines[22]}" =~ "|\ \ \ \  Merge: " ]] || false                                               # |\ \ \ \  Merge: xxx xxx
    [[ "${lines[23]}" =~ "| | | | | Author: " ]] || false                                              # | | | | | Author:
    [[ "${lines[24]}" =~ "| | | | | Date: " ]] || false                                                # | | | | | Date:
    [[ "${lines[25]}" =~ "| | | | |" ]] || false                                                       # | | | | |
    [[ "${lines[26]}" =~ "| | | | | 	merge b4" ]] || false                                            # | | | | | 	merge b4
    [[ "${lines[27]}" =~ "| | | | |" ]] || false                                                       # | | | | |
    [[ $(echo "${lines[28]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | | | |   commit " ]] || false      # * | | | |   commit xxx
    [[ "${lines[29]}" =~ "|\ \ \ \ \  Merge: " ]] || false                                             # |\ \ \ \ \  Merge: xxx xxx
    [[ "${lines[30]}" =~ "| | | | | | Author: " ]] || false                                            # | | | | | | Author:
    [[ "${lines[31]}" =~ "| | | | | | Date: " ]] || false                                              # | | | | | | Date:
    [[ "${lines[32]}" =~ "| | | | | |" ]] || false                                                     # | | | | | |
    [[ "${lines[33]}" =~ "| | | | | | 	merge b3" ]] || false                                          # | | | | | | 	merge b3
    [[ "${lines[34]}" =~ "| | | | | |" ]] || false                                                     # | | | | | |
    [[ $(echo "${lines[35]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | | | | |   commit " ]] || false    # * | | | | |   commit xxx
    [[ "${lines[36]}" =~ "|\ \ \ \ \ \  Merge: " ]] || false                                           # |\ \ \ \ \ \  Merge: xxx xxx
    [[ "${lines[37]}" =~ "| | | | | | | Author: " ]] || false                                          # | | | | | | | Author:
    [[ "${lines[38]}" =~ "| | | | | | | Date: " ]] || false                                            # | | | | | | | Date:
    [[ "${lines[39]}" =~ "| | | | | | |" ]] || false                                                   # | | | | | | |
    [[ "${lines[40]}" =~ "| | | | | | | 	merge b2" ]] || false                                        # | | | | | | | 	merge b2
    [[ "${lines[41]}" =~ "| | | | | | |" ]] || false                                                   # | | | | | | |
    [[ $(echo "${lines[42]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | | | | | |   commit " ]] || false  # * | | | | | |   commit xxx
    [[ "${lines[43]}" =~ "|\ \ \ \ \ \ \  Merge: " ]] || false                                         # |\ \ \ \ \ \ \  Merge: xxx xxx
    [[ "${lines[44]}" =~ "| | | | | | | | Author: " ]] || false                                        # | | | | | | | | Author:
    [[ "${lines[45]}" =~ "| | | | | | | | Date: " ]] || false                                          # | | | | | | | | Date:
    [[ "${lines[46]}" =~ "| | | | | | | |" ]] || false                                                 # | | | | | | | |
    [[ "${lines[47]}" =~ "| | | | | | | | 	merge b1" ]] || false                                      # | | | | | | | | 	merge b1
    [[ "${lines[48]}" =~ "| | | | | | | |" ]] || false                                                 # | | | | | | | |
    [[ $(echo "${lines[49]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | | | | | * | commit " ]] || false  # | | | | | | * | commit xxx
    [[ "${lines[50]}" =~ "| | | | | |/ /  Author: " ]] || false                                        # | | | | | |/ /  Author:
    [[ "${lines[51]}" =~ "| | | | |/| |   Date: " ]] || false                                          # | | | | |/| |   Date:
    [[ "${lines[52]}" =~ "| | | |/| | |" ]] || false                                                   # | | | |/| | |
    [[ "${lines[53]}" =~ "| | |/| | | |   	b7 1" ]] || false                                          # | | |/| | | |   	b7 1
    [[ "${lines[54]}" =~ "| |/| | | | |" ]] || false                                                   # | |/| | | | |
    [[ "${lines[55]}" =~ "|/| | | | | |" ]] || false                                                   # |/| | | | | |
    [[ $(echo "${lines[56]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | | | | | | commit " ]] || false    # * | | | | | | commit xxx
    [[ "${lines[57]}" =~ "| | | | | | | Author: " ]] || false                                          # | | | | | | | Author:
    [[ "${lines[58]}" =~ "| | | | | | | Date: " ]] || false                                            # | | | | | | | Date:
    [[ "${lines[59]}" =~ "| | | | | | |" ]] || false                                                   # | | | | | | |
    [[ "${lines[60]}" =~ "| | | | | | | 	main 1" ]] || false                                          # | | | | | | | 	main 1
    [[ "${lines[61]}" =~ "| | | | | | |" ]] || false                                                   # | | | | | | |
    [[ $(echo "${lines[62]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | | | | | * commit " ]] || false    # | | | | | | * commit xxx
    [[ "${lines[63]}" =~ "| | | | | |/  Author: " ]] || false                                          # | | | | | |/  Author:
    [[ "${lines[64]}" =~ "| | | | |/|   Date: " ]] || false                                            # | | | | |/|   Date:
    [[ "${lines[65]}" =~ "| | | |/| |" ]] || false                                                     # | | | |/| |
    [[ "${lines[66]}" =~ "| | |/| | |   	b6 1" ]] || false                                            # | | |/| | |   	b6 1
    [[ "${lines[67]}" =~ "| |/| | | |" ]] || false                                                     # | |/| | | |
    [[ "${lines[68]}" =~ "|/| | | | |" ]] || false                                                     # |/| | | | |
    [[ $(echo "${lines[69]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | | | | * commit " ]] || false      # | | | | | * commit xxx
    [[ "${lines[70]}" =~ "| | | | |/  Author: " ]] || false                                            # | | | | |/  Author:
    [[ "${lines[71]}" =~ "| | | |/|   Date: " ]] || false                                              # | | | |/|   Date:
    [[ "${lines[72]}" =~ "| | |/| |" ]] || false                                                       # | | |/| |
    [[ "${lines[73]}" =~ "| |/| | |   	b5 1" ]] || false                                              # | |/| | |   	b5 1
    [[ "${lines[74]}" =~ "|/| | | |" ]] || false                                                       # |/| | | |
    [[ $(echo "${lines[75]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | | | * commit " ]] || false        # | | | | * commit xxx
    [[ "${lines[76]}" =~ "| | | |/  Author: " ]] || false                                              # | | | |/  Author:
    [[ "${lines[77]}" =~ "| | |/|   Date: " ]] || false                                                # | | |/|   Date:
    [[ "${lines[78]}" =~ "| |/| |" ]] || false                                                         # | |/| |
    [[ "${lines[79]}" =~ "|/| | |   	b4 1" ]] || false                                                # |/| | |   	b4 1
    [[ "${lines[80]}" =~ "| | | |" ]] || false                                                         # | | | |
    [[ $(echo "${lines[81]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | | * commit " ]] || false          # | | | * commit
    [[ "${lines[82]}" =~ "| | |/  Author: " ]] || false                                                # | | |/  Author:
    [[ "${lines[83]}" =~ "| |/|   Date: " ]] || false                                                  # | |/|   Date:
    [[ "${lines[84]}" =~ "|/| |" ]] || false                                                           # |/| |
    [[ "${lines[85]}" =~ "| | |   	b3 1" ]] || false                                                  # | | |   	b3 1
    [[ "${lines[86]}" =~ "| | |" ]] || false                                                           # | | |
    [[ $(echo "${lines[87]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | * commit " ]] || false            # | | * commit
    [[ "${lines[88]}" =~ "| |/  Author: " ]] || false                                                  # | |/  Author:
    [[ "${lines[89]}" =~ "|/|   Date: " ]] || false                                                    # |/|   Date:
    [[ "${lines[90]}" =~ "| |" ]] || false                                                             # | |
    [[ "${lines[91]}" =~ "| |   	b2 1" ]] || false                                                    # | |   	b2 1
    [[ "${lines[92]}" =~ "| |" ]] || false                                                             # | |
    [[ $(echo "${lines[93]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| * commit " ]] || false              # | * commit
    [[ "${lines[94]}" =~ "|/  Author: " ]] || false                                                    # |/  Author:
    [[ "${lines[95]}" =~ "|   Date: " ]] || false                                                      # |   Date:
    [[ "${lines[96]}" =~ "|" ]] || false                                                               # |
    [[ "${lines[97]}" =~ "|   	b1 1" ]] || false                                                      # |   	b1 1
    [[ "${lines[98]}" =~ "|" ]] || false                                                               # |
    [[ $(echo "${lines[99]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* commit " ]] || false                # * commit
    [[ "${lines[100]}" =~ "| Author: " ]] || false                                                     # | Author:
    [[ "${lines[101]}" =~ "| Date: " ]] || false                                                       # | Date:
    [[ "${lines[102]}" =~ "|" ]] || false                                                              # |
    [[ "${lines[103]}" =~ "| 	Initialize data repository" ]] || false                                  # | 	Initialize data repository
    [[ "${lines[104]}" =~ "|" ]] || false                                                              # |
}

@test "log-graph: merge left in column before node edge" {
  dolt branch b1
  dolt branch b2
  dolt commit --allow-empty -m "main 1"
  dolt checkout b2
  dolt commit --allow-empty -m "b2 1"
  dolt checkout b1
  dolt merge --no-ff main -m "merge main"
  dolt checkout main
  dolt merge --no-ff b1 -m "merge b1"
  dolt merge --no-ff b2 -m "merge b2"

  run dolt log --graph
  [ $status -eq 0 ]
  [ "${#lines[@]}" -eq 39 ]
  [[ $(echo "${lines[0]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "*   commit " ]] || false              # *   commit xxx
  [[ "${lines[1]}" =~ "|\  Merge: " ]] || false                                                     # |\  Merge: xxx xxx
  [[ "${lines[2]}" =~ "| | Author: " ]] || false                                                    # | | Author:
  [[ "${lines[3]}" =~ "| | Date: " ]] || false                                                      # | | Date:
  [[ "${lines[4]}" =~ "| |" ]] || false                                                             # | |
  [[ "${lines[5]}" =~ "| | 	merge b2" ]] || false                                                   # | | 	merge b6
  [[ "${lines[6]}" =~ "| |" ]] || false                                                             # | |
  [[ $(echo "${lines[7]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* |   commit " ]] || false            # * |   commit xxx
  [[ "${lines[8]}" =~ "|\ \  Merge: " ]] || false                                                   # |\ \  Merge: xxx xxx
  [[ "${lines[9]}" =~ "| | | Author: " ]] || false                                                  # | | | Author:
  [[ "${lines[10]}" =~ "| | | Date: " ]] || false                                                   # | | | Date:
  [[ "${lines[11]}" =~ "| | |" ]] || false                                                          # | | |
  [[ "${lines[12]}" =~ "| | | 	merge b1" ]] || false                                               # | | | 	merge b1
  [[ "${lines[13]}" =~ "| | |" ]] || false                                                          # | | |
  [[ $(echo "${lines[14]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| * |   commit " ]] || false         # | * |   commit xxx
  [[ "${lines[15]}" =~ "| |\ \  Merge: " ]] || false                                                # | |\ \  Merge: xxx xxx
  [[ "${lines[16]}" =~ "| | | | Author: " ]] || false                                               # | | | | Author
  [[ "${lines[17]}" =~ "| | | | Date: " ]] || false                                                 # | | | | Date:
  [[ "${lines[18]}" =~ "| | | |" ]] || false                                                        # | | | |
  [[ "${lines[19]}" =~ "| | | | 	merge main" ]] || false                                           # | | | | 	merge main
  [[ "${lines[20]}" =~ "| | | |" ]] || false                                                        # | | | |
  [[ $(echo "${lines[21]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | | * commit " ]] || false         # | | | * commit xxx
  [[ "${lines[22]}" =~ "| |/ /  Author: " ]] || false                                               # | |/ /  Author:
  [[ "${lines[23]}" =~ "|/|/    Date: " ]] || false                                                 # |/|/    Date:
  [[ "${lines[24]}" =~ "| |" ]] || false                                                            # | |
  [[ "${lines[25]}" =~ "| |     	b2 1" ]] || false                                                 # | |     	b2 1
  [[ "${lines[26]}" =~ "| |" ]] || false                                                            # | |
  [[ $(echo "${lines[27]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | commit " ]] || false             # * | commit xxx
  [[ "${lines[28]}" =~ "|/  Author: " ]] || false                                                   # |/  Author:
  [[ "${lines[29]}" =~ "|   Date: " ]] || false                                                     # |   Date:
  [[ "${lines[30]}" =~ "|" ]] || false                                                              # |
  [[ "${lines[31]}" =~ "|   	main 1" ]] || false                                                   # |   	main 1
  [[ "${lines[32]}" =~ "|" ]] || false                                                              # |
  [[ $(echo "${lines[33]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* commit " ]] || false               # * commit
  [[ "${lines[34]}" =~ "| Author: " ]] || false                                                     # | Author:
  [[ "${lines[35]}" =~ "| Date: " ]] || false                                                       # | Date:
  [[ "${lines[36]}" =~ "|" ]] || false                                                              # |
  [[ "${lines[37]}" =~ "| 	Initialize data repository" ]] || false                                 # | 	Initialize data repository
  [[ "${lines[38]}" =~ "|" ]] || false                                                              # |
}

@test "log-graph: merging edges in merge commit" {
    dolt branch b1
    dolt branch b2
    dolt checkout -b b3
    dolt commit --allow-empty -m "b3 1"
    dolt checkout main
    dolt commit --allow-empty -m "main 1"
    dolt checkout b2
    dolt merge --no-ff b3 -m "merge b3"
    dolt commit --allow-empty -m "b2 1"
    dolt checkout b1
    dolt merge --no-ff main -m "merge main"
    dolt checkout main
    dolt merge --no-ff b1 -m "merge b1"
    dolt merge --no-ff b2 -m "merge b2"

    run dolt log --graph
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 52 ]
    [[ $(echo "${lines[0]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "*   commit " ]] || false            # *   commit xxx
  [[ "${lines[1]}" =~ "|\  Merge: " ]] || false                                                     # |\  Merge: xxx xxx
  [[ "${lines[2]}" =~ "| | Author: " ]] || false                                                    # | | Author:
  [[ "${lines[3]}" =~ "| | Date: " ]] || false                                                      # | | Date:
  [[ "${lines[4]}" =~ "| |" ]] || false                                                             # | |
  [[ "${lines[5]}" =~ "| | 	merge b2" ]] || false                                                   # | | 	merge b6
  [[ "${lines[6]}" =~ "| |" ]] || false                                                             # | |
  [[ $(echo "${lines[7]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* |   commit " ]] || false            # * |   commit xxx
  [[ "${lines[8]}" =~ "|\ \  Merge: " ]] || false                                                   # |\ \  Merge: xxx xxx
  [[ "${lines[9]}" =~ "| | | Author: " ]] || false                                                  # | | | Author:
  [[ "${lines[10]}" =~ "| | | Date: " ]] || false                                                   # | | | Date:
  [[ "${lines[11]}" =~ "| | |" ]] || false                                                          # | | |
  [[ "${lines[12]}" =~ "| | | 	merge b1" ]] || false                                               # | | | 	merge b1
  [[ "${lines[13]}" =~ "| | |" ]] || false                                                          # | | |
  [[ $(echo "${lines[14]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | * commit " ]] || false           # | | * commit xxx
  [[ "${lines[15]}" =~ "| | | Author: " ]] || false                                                 # | | | Author:
  [[ "${lines[16]}" =~ "| | | Date: " ]] || false                                                   # | | | Date:
  [[ "${lines[17]}" =~ "| | |" ]] || false                                                          # | | |
  [[ "${lines[18]}" =~ "| | | 	b2 1" ]] || false                                                   # | | | 	b2 1
  [[ "${lines[19]}" =~ "| | |" ]] || false                                                          # | | |
  [[ $(echo "${lines[20]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| * |   commit " ]] || false         # | * |   commit xxx
  [[ "${lines[21]}" =~ "| |\ \  Merge: " ]] || false                                                # | |\ \  Merge:
  [[ "${lines[22]}" =~ "| | | | Author: " ]] || false                                               # | | | | Author:
  [[ "${lines[23]}" =~ "| | | | Date: " ]] || false                                                 # | | | | Date:
  [[ "${lines[24]}" =~ "| | | |" ]] || false                                                        # | | | |
  [[ "${lines[25]}" =~ "| | | | 	merge main" ]] || false                                           # | | | | 	merge main
  [[ "${lines[26]}" =~ "| | | |" ]] || false                                                        # | | | |
  [[ $(echo "${lines[27]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| | | * commit " ]] || false         # | | | * commit xxx
  [[ "${lines[28]}" =~ "| |/ /| Merge: " ]] || false                                                # | |/ /| Merge: xxx xxx
  [[ "${lines[29]}" =~ "|/| | | Author: " ]] || false                                               # |/| | | Author:
  [[ "${lines[30]}" =~ "| | | | Date: " ]] || false                                                 # | | | | Date:
  [[ "${lines[31]}" =~ "| | | |" ]] || false                                                        # | | | |
  [[ "${lines[32]}" =~ "| | | | 	merge b3" ]] || false                                             # | | | | 	merge b3
  [[ "${lines[33]}" =~ "| | | |" ]] || false                                                        # | | | |
  [[ $(echo "${lines[34]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* | | | commit " ]] || false         # * | | | commit xxx
  [[ "${lines[35]}" =~ "|/ / /  Author: " ]] || false                                               # |/ / /  Author:
  [[ "${lines[36]}" =~ "|/ /    Date: " ]] || false                                                 # |/ /    Date:
  [[ "${lines[37]}" =~ "| |" ]] || false                                                            # | |
  [[ "${lines[38]}" =~ "| |     	main 1" ]] || false                                               # | |     	main 1
  [[ "${lines[39]}" =~ "| |" ]] || false                                                            # | |
  [[ $(echo "${lines[40]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "| * commit " ]] || false             # | * commit
  [[ "${lines[41]}" =~ "|/  Author: " ]] || false                                                   # |/  Author:
  [[ "${lines[42]}" =~ "|   Date: " ]] || false                                                     # |   Date:
  [[ "${lines[43]}" =~ "|" ]] || false                                                              # |
  [[ "${lines[44]}" =~ "|   	b3 1" ]] || false                                                     # |   	b3 1
  [[ "${lines[45]}" =~ "|" ]] || false                                                              # |
  [[ $(echo "${lines[46]}" | sed -E 's/\x1b\[[0-9;]*m//g') =~ "* commit " ]] || false               # * commit
  [[ "${lines[47]}" =~ "| Author: " ]] || false                                                     # | Author:
  [[ "${lines[48]}" =~ "| Date: " ]] || false                                                       # | Date:
  [[ "${lines[49]}" =~ "|" ]] || false                                                              # |
  [[ "${lines[50]}" =~ "| 	Initialize data repository" ]] || false                                 # | 	Initialize data repository
  [[ "${lines[51]}" =~ "|" ]] || false                                                              # |
}

@test "log-graph: merge with shared parent" {
    # broken
    dolt branch b1
    dolt branch b2
    dolt commit --allow-empty -m "main 1"
    dolt checkout b1
    dolt merge --no-ff main -m "merge main"
    dolt checkout main
    dolt merge --no-ff b1 -m "merge b1"
    dolt merge --no-ff b2 -m "merge b2"

    run dolt log --graph
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 91 ]
}

@test "log-graph: diagonal edge in merge commit " {
    dolt checkout -b b1
    dolt commit --allow-empty -m "b1 1"
    dolt checkout main
    dolt checkout -b b2
    dolt commit --allow-empty -m "b2 1"
    dolt checkout main
    dolt merge --no-ff b1 -m "merge b1"
    dolt branch b3
    dolt merge --no-ff b2 -m "merge b2"
    dolt checkout b3
    dolt commit --allow-empty -m "b3 1"
    dolt checkout main
    dolt merge --no-ff b3 -m "merge b3"

    run dolt log --graph
    [ $status -eq 0 ]
    [ "${#lines[@]}" -eq 91 ]
}
