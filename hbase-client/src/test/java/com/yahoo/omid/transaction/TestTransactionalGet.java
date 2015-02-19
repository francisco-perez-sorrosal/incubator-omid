package com.yahoo.omid.transaction;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class TestTransactionalGet extends OmidTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(TestTransactionalGet.class);

    byte[] rowId = Bytes.toBytes("row1");
    byte[] fam = Bytes.toBytes(TEST_FAMILY);
    byte[] qual = Bytes.toBytes("col1");
    byte[] data1 = Bytes.toBytes("testWrite-1");
    byte[] data2 = Bytes.toBytes("testWrite-2");

    @Test
    public void testTxGetObtainsCorrectResults()
            throws Exception {

        // Create required components
        TransactionManager tm = newTransactionManager();
        TTable tt = new TTable(hbaseConf, TEST_TABLE);
        TOpsManager tOpsManager = TOpsManager.newBuilder(hbaseConf).build(); // Comp under test

        // First, create a transaction that puts the initial data
        Transaction tx1 = tm.begin();

        Put row = new Put(rowId);
        row.add(fam, qual, data1);
        tt.put(tx1, row);

        tm.commit(tx1);

        // Start a second transaction writing the same data...
        Transaction tx2 = tm.begin();

        row = new Put(rowId);
        row.add(fam, qual, data2);
        tt.put(tx2, row);

        // ...but make the transactional get before committing
        Result tGetResult = tOpsManager.get(TEST_TABLE, new Get(rowId));
        byte[] val = tGetResult.getValue(fam, qual);
        assertTrue(Bytes.equals(data1, val));

        // Then commit, perform another transactional get and check
        // the new result is correct
        tm.commit(tx2);

        tGetResult = tOpsManager.get(TEST_TABLE, new Get(rowId));
        val = tGetResult.getValue(fam, qual);
        assertTrue(Bytes.equals(data2, val));

        tt.close();

        // Close the component under test
        tOpsManager.close();

        // After closing this should throw an exception
        try {
            tOpsManager.get(TEST_TABLE, new Get(Bytes.toBytes("ROW")));
            fail();
        } catch (IllegalStateException e) {
            // Expected
        }
    }


}
