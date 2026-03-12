# Check cluster I/O offload observability counters

start_cluster 3 0 {tags {external:skip cluster}} {

test "Cluster should start ok" {
    wait_for_cluster_state ok
}

test "Cluster I/O offload stats reflect io-threads configuration" {
    # Determine if io-threads are active (>1 means offload is enabled)
    set io_threads [lindex [R 0 config get io-threads] 1]

    for {set j 0} {$j < 3} {incr j} {
        set info [R $j info stats]
        set reads_offloaded [getInfoProperty $info cluster_io_reads_offloaded]
        set sync_fallbacks [getInfoProperty $info cluster_io_sync_fallbacks]

        if {$io_threads > 1} {
            assert {$reads_offloaded > 0}
        } else {
            assert {$sync_fallbacks > 0}
        }

        assert_equal 0 [getInfoProperty $info cluster_io_async_closed_links]
        assert_equal 0 [getInfoProperty $info cluster_io_queued_inbound_packets]
    }
}

test "Cluster I/O offload stats reset via CONFIG RESETSTAT" {
    R 0 config resetstat
    set info [R 0 info stats]
    assert_equal 0 [getInfoProperty $info cluster_io_reads_offloaded]
    assert_equal 0 [getInfoProperty $info cluster_io_sync_fallbacks]
    assert_equal 0 [getInfoProperty $info cluster_io_async_closed_links]
    assert_equal 0 [getInfoProperty $info cluster_io_queued_inbound_packets]
}

} ;# start_cluster
