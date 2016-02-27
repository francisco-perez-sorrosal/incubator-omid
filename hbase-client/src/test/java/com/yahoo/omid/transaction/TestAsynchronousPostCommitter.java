package com.yahoo.omid.transaction;

import com.google.common.base.Optional;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.metrics.NullMetricsProvider;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.ITestContext;
import org.testng.annotations.Test;

import java.util.concurrent.CountDownLatch;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test(groups = "sharedHBase")
public class TestAsynchronousPostCommitter extends OmidTestBase {

    private static final byte[] family = Bytes.toBytes(TEST_FAMILY);
    private static final byte[] nonExistentFamily = Bytes.toBytes("non-existent");
    private static final byte[] qualifier = Bytes.toBytes("test-qual");

    byte[] row1 = Bytes.toBytes("test-is-committed1");
    byte[] row2 = Bytes.toBytes("test-is-committed2");

    @Test(timeOut = 30_000)
    public void testPostCommitActionsAreCalledAsynchronously(ITestContext context) throws Exception {

        CommitTable.Client commitTableClient = getCommitTable(context).getClient();

        PostCommitActions syncPostCommitter =
                spy(new HBaseSyncPostCommitter(new NullMetricsProvider(), commitTableClient));
        PostCommitActions asyncPostCommitter = new HBaseAsyncPostCommitter(syncPostCommitter);

        TransactionManager tm = newTransactionManager(context, asyncPostCommitter);

        final CountDownLatch beforeUpdatingShadowCellsLatch = new CountDownLatch(1);
        final CountDownLatch afterUpdatingShadowCellsLatch = new CountDownLatch(1);

        doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                try {
                    beforeUpdatingShadowCellsLatch.await();
                    invocation.callRealMethod();
                    afterUpdatingShadowCellsLatch.countDown();
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                }
                return null;
            }
        }).when(syncPostCommitter).updateShadowCells(any(AbstractTransaction.class));

        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {

            // Execute tx with async post commit actions
            Transaction tx1 = tm.begin();

            Put put1 = new Put(row1);
            put1.add(family, qualifier, Bytes.toBytes("hey!"));
            txTable.put(tx1, put1);
            Put put2 = new Put(row2);
            put2.add(family, qualifier, Bytes.toBytes("hou!"));
            txTable.put(tx1, put2);

            tm.commit(tx1);

            long tx1Id = tx1.getTransactionId();

            // As we have paused the update of shadow cells, the shadow cells shouldn't be there yet
            assertFalse(CellUtils.hasShadowCell(row1, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));
            assertFalse(CellUtils.hasShadowCell(row2, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));

            // Commit Table should contain an entry for the transaction
            Optional<CommitTable.CommitTimestamp> commitTimestamp = commitTableClient.getCommitTimestamp(tx1Id).get();
            assertTrue(commitTimestamp.isPresent());
            assertTrue(commitTimestamp.get().isValid());
            assertEquals(commitTimestamp.get().getValue(), ((AbstractTransaction) tx1).getCommitTimestamp());

            // Read from row1 and row2 in a different Tx and check that result is the data written by tx1 despite the
            // post commit actions have not been executed yet (the shadow cells healing process should make its work)
            Transaction tx2 = tm.begin();
            Get get1 = new Get(row1);
            Result result = txTable.get(tx2, get1);
            byte[] value =  result.getValue(family, qualifier);
            assertNotNull(value);
            assertEquals("hey!", Bytes.toString(value));

            Get get2 = new Get(row2);
            result = txTable.get(tx2, get2);
            value = result.getValue(family, qualifier);
            assertNotNull(value);
            assertEquals("hou!", Bytes.toString(value));

            // Then, we continue with the update of shadow cells and we wait till completed
            beforeUpdatingShadowCellsLatch.countDown();
            afterUpdatingShadowCellsLatch.await();

            // Finally, we check that the shadow cells are there
            assertTrue(CellUtils.hasShadowCell(row1, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));
            assertTrue(CellUtils.hasShadowCell(row2, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));

            verify(syncPostCommitter, times(1)).updateShadowCells(any(AbstractTransaction.class));
            verify(syncPostCommitter, times(1)).removeCommitTableEntry(any(AbstractTransaction.class));

            // Commit Table should NOT contain the entry for the transaction anymore
            commitTimestamp = commitTableClient.getCommitTimestamp(tx1Id).get();
            assertFalse(commitTimestamp.isPresent());

        }

    }

    @Test(timeOut = 30_000)
    public void testNoAsyncPostActionsAreCalled(ITestContext context) throws Exception {

        CommitTable.Client commitTableClient = getCommitTable(context).getClient();

        PostCommitActions syncPostCommitter =
                spy(new HBaseSyncPostCommitter(new NullMetricsProvider(), commitTableClient));
        PostCommitActions asyncPostCommitter = new HBaseAsyncPostCommitter(syncPostCommitter);

        TransactionManager tm = newTransactionManager(context, asyncPostCommitter);

        final CountDownLatch updateShadowCellsCalledLatch = new CountDownLatch(1);
        final CountDownLatch removeCommitTableEntryCalledLatch = new CountDownLatch(1);

        // Simulate shadow cells are not updated and commit table is not clean
        doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                // Do not invoke real method simulating a fail of the shadow cells update
                updateShadowCellsCalledLatch.countDown();
                return null;
            }
        }).when(syncPostCommitter).updateShadowCells(any(AbstractTransaction.class));

        doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                // Do not invoke real method simulating a fail of the async clean of commit table entry
                removeCommitTableEntryCalledLatch.countDown();
                return null;
            }
        }).when(syncPostCommitter).removeCommitTableEntry(any(AbstractTransaction.class));


        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {

            // Execute tx with async post commit actions
            Transaction tx1 = tm.begin();

            Put put1 = new Put(row1);
            put1.add(family, qualifier, Bytes.toBytes("hey!"));
            txTable.put(tx1, put1);
            Put put2 = new Put(row2);
            put2.add(family, qualifier, Bytes.toBytes("hou!"));
            txTable.put(tx1, put2);

            tm.commit(tx1);

            long tx1Id = tx1.getTransactionId();

            // As we have paused the update of shadow cells, the shadow cells shouldn't be there yet
            assertFalse(CellUtils.hasShadowCell(row1, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));
            assertFalse(CellUtils.hasShadowCell(row2, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));

            // We continue with when both methods have been completed without doing nothing
            updateShadowCellsCalledLatch.await();
            removeCommitTableEntryCalledLatch.await();

            // Finally, we check that the shadow cells are NOT there...
            assertFalse(CellUtils.hasShadowCell(row1, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));
            assertFalse(CellUtils.hasShadowCell(row2, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));

            // ... and the commit table entry has NOT been cleaned
            Optional<CommitTable.CommitTimestamp> commitTimestamp = commitTableClient.getCommitTimestamp(tx1Id).get();
            assertTrue(commitTimestamp.isPresent());
            assertTrue(commitTimestamp.get().isValid());
        }

    }

    @Test(timeOut = 30_000)
    public void testOnlyShadowCellsUpdateIsExecuted(ITestContext context) throws Exception {

        CommitTable.Client commitTableClient = getCommitTable(context).getClient();

        PostCommitActions syncPostCommitter =
                spy(new HBaseSyncPostCommitter(new NullMetricsProvider(), commitTableClient));
        PostCommitActions asyncPostCommitter = new HBaseAsyncPostCommitter(syncPostCommitter);

        TransactionManager tm = newTransactionManager(context, asyncPostCommitter);

        final CountDownLatch removeCommitTableEntryCalledLatch = new CountDownLatch(1);

        doAnswer(new Answer<Void>() {
            public Void answer(InvocationOnMock invocation) {
                // Do not invoke real method simulating a fail of the async clean of commit table entry
                removeCommitTableEntryCalledLatch.countDown();
                return null;
            }
        }).when(syncPostCommitter).removeCommitTableEntry(any(AbstractTransaction.class));


        try (TTable txTable = new TTable(hbaseConf, TEST_TABLE)) {

            // Execute tx with async post commit actions
            Transaction tx1 = tm.begin();

            Put put1 = new Put(row1);
            put1.add(family, qualifier, Bytes.toBytes("hey!"));
            txTable.put(tx1, put1);
            Put put2 = new Put(row2);
            put2.add(family, qualifier, Bytes.toBytes("hou!"));
            txTable.put(tx1, put2);

            tm.commit(tx1);

            long tx1Id = tx1.getTransactionId();

            // As we have paused the update of shadow cells, the shadow cells shouldn't be there yet
            assertFalse(CellUtils.hasShadowCell(row1, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));
            assertFalse(CellUtils.hasShadowCell(row2, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));

            // We continue with when the unsuccessful call of the method for cleaning commit table has been invoked
            removeCommitTableEntryCalledLatch.await();

            // Finally, we check that the shadow cells are there...
            assertTrue(CellUtils.hasShadowCell(row1, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));
            assertTrue(CellUtils.hasShadowCell(row2, family, qualifier, tx1Id, new TTableCellGetterAdapter(txTable)));

            // ... and the commit table entry has NOT been cleaned
            Optional<CommitTable.CommitTimestamp> commitTimestamp = commitTableClient.getCommitTimestamp(tx1Id).get();
            assertTrue(commitTimestamp.isPresent());
            assertTrue(commitTimestamp.get().isValid());
        }

    }

}
