# Check cluster I/O offload observability counters

start_cluster 3 0 {tags {external:skip cluster}} {

test "Cluster should start ok" {
    wait_for_cluster_state ok
}

test "Cluster I/O offload stats are present in INFO stats for all nodes" {
    for {set j 0} {$j < 3} {incr j} {
        set info [R $j info stats]
        # Verify all cluster I/O offload counters are present and zero
        # (read dispatch is not yet wired into event handlers)
        assert_equal 0 [getInfoProperty $info cluster_io_reads_offloaded]
        assert_equal 0 [getInfoProperty $info cluster_io_sync_fallbacks]
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
