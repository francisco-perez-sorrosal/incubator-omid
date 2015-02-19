package com.yahoo.omid.transaction;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.fail;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.yahoo.omid.committable.hbase.HBaseCommitTable;

public class TestTOpsManager extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestTOpsManager.class);

    @Test
    public void testTTableCacheWorksAsExpected() throws Exception {

        // Create required components
        TOpsManager tOpsManager = TOpsManager.newBuilder(hbaseConf).build(); // Comp under test

        // Test trying to get an instance from cache of a non-existent table
        // in HBase throws an exception
        try{
            tOpsManager.getTTableFromCache("non-existent-table");
            fail();
        } catch (TransactionException e) {
            // Expected
        }

        // Test we get the right instances when using the cache
        TTable tTableFromCache = tOpsManager.getTTableFromCache(TEST_TABLE);
        TTable sameTTableFromCache = tOpsManager.getTTableFromCache(TEST_TABLE);
        TTable otherTTableFromCache = tOpsManager.getTTableFromCache(HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME);
        assertEquals(tTableFromCache, sameTTableFromCache, "Should be the same instance");
        assertNotEquals(tTableFromCache, otherTTableFromCache, "Should be different instances");

        tOpsManager.close();

        TTable shouldBeDifferentTTableFromCache = tOpsManager.getTTableFromCache(TEST_TABLE);
        assertNotEquals(tTableFromCache,
                        shouldBeDifferentTTableFromCache,
                        "Should be different instances due to cache invalidation when closing TOpsManager");

    }


}
