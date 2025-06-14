# Test cluster debug message extension propagation

proc debug_msg_sum_reached {} {
    set nodecount [llength $::servers]
    for {set i 0} {$i < $nodecount} {incr i} {
        set msgmap [R $i DEBUG CLUSTER-MSG-GET]
        set sum 0
        foreach {id val} $msgmap {
            if {$val eq {}} {
                continue
            }
            incr sum $val
        }
        puts "Node $i: sum=$sum"
        flush stdout
        if {$sum != $nodecount} {
            return 0
        }
    }
    return 1
}

start_cluster 100 100 {tags {external:skip cluster}} {

    test "Cluster is up" {
        puts "Waiting for cluster to be up..."
        wait_for_cluster_state ok
    }

    test "Debug message propagation" {
        set nodecount [llength $::servers]
        set start_time [clock milliseconds]

        for {set i 0} {$i < $nodecount} {incr i} {
            R $i DEBUG CLUSTER-MSG-SET 1
        }

        wait_for_condition 1000 50 {
            [debug_msg_sum_reached]
        } else {
            fail "debug message not propagated across cluster"
        }

        set end_time [clock milliseconds]
        set duration [expr {$end_time - $start_time}]
        puts "Time to propagate debug message: $duration ms"
    }
}


