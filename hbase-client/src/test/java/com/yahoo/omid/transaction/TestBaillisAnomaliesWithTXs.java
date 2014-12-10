package com.yahoo.omid.transaction;

import static com.yahoo.omid.tsoclient.TSOClient.TSO_HOST_CONFKEY;
import static com.yahoo.omid.tsoclient.TSOClient.TSO_PORT_CONFKEY;
import static org.apache.hadoop.hbase.HConstants.HBASE_CLIENT_RETRIES_NUMBER;
import static org.slf4j.LoggerFactory.getLogger;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.yahoo.omid.TestUtils;
import com.yahoo.omid.committable.CommitTable;
import com.yahoo.omid.committable.hbase.CreateTable;
import com.yahoo.omid.committable.hbase.HBaseCommitTable;
import com.yahoo.omid.tso.TSOMockModule;
import com.yahoo.omid.tso.TSOServer;
import com.yahoo.omid.tso.TSOServerCommandLineConfig;
import com.yahoo.omid.tsoclient.TSOClient;

/**
 * These tests try to analyze the transactional anomalies described by P. Baillis et al. in
 * http://arxiv.org/pdf/1302.0309.pdf
 *
 * These tests try to model what project Hermitage is trying to do to compare the behavior of different DBMSs on these
 * anomalies depending on the different isolation levels they offer. For more info on the Hermitage project, please
 * refer to: https://github.com/ept/hermitage
 *
 * Transactional histories have been translated to HBase from the ones done for Postgresql in the Hermitage project:
 * https://github.com/ept/hermitage/blob/master/postgres.md
 *
 * The "repeatable read" Postgresql isolation level is equivalent to "snapshot isolation", so we include the experiments
 * for that isolation level
 *
 * With HBase 0.98 interfaces is not possible to execute updates/deletes based on predicates so the examples here are
 * not exactly the same as in Postgres
 */
public class TestBaillisAnomaliesWithTXs {

    private static final Logger LOG = getLogger(TestBaillisAnomaliesWithTXs.class);

    // Data used in the tests
    byte[] famName = Bytes.toBytes(TEST_FAMILY);
    byte[] colName = Bytes.toBytes(TEST_COLUMN);

    byte[] rowId1 = Bytes.toBytes("row1");
    byte[] rowId2 = Bytes.toBytes("row2");
    byte[] rowId3 = Bytes.toBytes("row3");

    byte[] dataValue1 = Bytes.toBytes(10);
    byte[] dataValue2 = Bytes.toBytes(20);
    byte[] dataValue3 = Bytes.toBytes(30);

    private TransactionManager tm;
    private TTable txTable;

    @Test
    public void testSIPreventsPredicateManyPrecedersForReadPredicates() throws Exception {
        // TX History for PMP for Read Predicate:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where value = 30; -- T1. Returns nothing
        // insert into test (id, value) values(3, 30); -- T2
        // commit; -- T2
        // select * from test where value % 3 = 0; -- T1. Still returns nothing
        // commit; -- T1

        // 0) Start transactions
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        // 1) select * from test where value = 30; -- T1. Returns nothing
        Scan scan = new Scan();
        Filter f = new SingleColumnValueFilter(famName, colName, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(30));
        scan.setFilter(f);
        ResultScanner tx1Scanner = txTable.getScanner(tx1, scan);
        assertNull(tx1Scanner.next());

        // 2) insert into test (id, value) values(3, 30); -- T2
        Put newRow = new Put(rowId3);
        newRow.add(famName, colName, dataValue3);
        txTable.put(tx2, newRow);

        // 3) Commit TX 2
        tm.commit(tx2);

        // 4) select * from test where value % 3 = 0; -- T1. Still returns nothing
        tx1Scanner = txTable.getScanner(tx1, scan);
        assertNull(tx1Scanner.next());

        // 5) Commit TX 1
        tm.commit(tx1);
    }

    @Test
    public void testSIPreventsPredicateManyPrecedersForWritePredicates() throws Exception {
        // TX History for PMP for Write Predicate:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // update test set value = value + 10; -- T1
        // delete from test where value = 20; -- T2, BLOCKS
        // commit; -- T1. T2 now prints out "ERROR: could not serialize access due to concurrent update"
        // abort; -- T2. There's nothing else we can do, this transaction has failed

        // 0) Start transactions
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        // 1) update test set value = value + 10; -- T1
        Scan updateScan = new Scan();
        ResultScanner tx1Scanner = txTable.getScanner(tx2, updateScan);
        Result updateRes = tx1Scanner.next();
        int count = 0;
        while (updateRes != null) {
            LOG.info("RESSS {}", updateRes);
            Put row = new Put(updateRes.getRow());
            int val = Bytes.toInt(updateRes.getValue(famName, colName));
            LOG.info("Updating row id {} with value {}", Bytes.toString(updateRes.getRow()), val);
            row.add(famName, colName, Bytes.toBytes(val + 10));
            txTable.put(tx1, row);
            updateRes = tx1Scanner.next();
            count++;
        }
        assertEquals(count, 2);

        // 2) delete from test where value = 20; -- T2, BLOCKS
        Scan scan = new Scan();
        Filter f = new SingleColumnValueFilter(famName, colName, CompareFilter.CompareOp.EQUAL, Bytes.toBytes(20));
        scan.setFilter(f);
        ResultScanner tx2Scanner = txTable.getScanner(tx2, scan);
        // assertEquals(tx2Scanner.next(100).length, 1);
        Result res = tx2Scanner.next();
        int count20 = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Deleting row id {} with value {}", Bytes.toString(res.getRow()),
                    Bytes.toInt(res.getValue(famName, colName)));
            Delete delete20 = new Delete(res.getRow());
            txTable.delete(tx2, delete20);
            res = tx2Scanner.next();
            count20++;
        }
        assertEquals(count20, 1);
//        int count20 = 0;
//        for (Result res : tx2Scanner) {
//            LOG.info("RESSS {}", res);
//            LOG.info("Deleting row id {} with value {}", Bytes.toString(res.getRow()),
//                    Bytes.toString(res.getValue(famName, colName)));
//            Delete delete20 = new Delete(res.getRow());
//            txTable.delete(tx2, delete20);
//            res = tx2Scanner.next();
//            count20++;
//        }
//        assertEquals(count20, 1);

        // 3) commit TX 1
        tm.commit(tx1);

        tx2Scanner = txTable.getScanner(tx2, scan);
        assertNull(tx2Scanner.next());

        // 4) commit TX 2 -> Should be rolled-back
        try {
            tm.commit(tx2);
            fail();
        } catch (RollbackException e) {
            // Expected
        }

    }

    @Test
    public void testSIPreventsLostUpdates() throws Exception {
        // TX History for P4:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where id = 1; -- T1
        // select * from test where id = 1; -- T2
        // update test set value = 11 where id = 1; -- T1
        // update test set value = 11 where id = 1; -- T2, BLOCKS
        // commit; -- T1. T2 now prints out "ERROR: could not serialize access due to concurrent update"
        // abort;  -- T2. There's nothing else we can do, this transaction has failed

        // 0) Start transactions
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        Scan scan = new Scan(rowId1, rowId1);
        scan.addColumn(famName, colName);

        // 1) select * from test where id = 1; -- T1
        ResultScanner tx1Scanner = txTable.getScanner(tx1, scan);
        Result res = tx1Scanner.next();
        int count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                    Bytes.toString(res.getValue(famName, colName)));
            assertEquals(res.getRow(), rowId1);
            assertEquals(res.getValue(famName, colName), dataValue1);
            res = tx1Scanner.next();
            count++;
        }
        assertEquals(count, 1);

        // 2) select * from test where id = 1; -- T2
        ResultScanner tx2Scanner = txTable.getScanner(tx2, scan);
        res = tx2Scanner.next();
        count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                    Bytes.toString(res.getValue(famName, colName)));
            assertEquals(res.getRow(), rowId1);
            assertEquals(res.getValue(famName, colName), dataValue1);
            res = tx2Scanner.next();
            count++;
        }
        assertEquals(count, 1);

        // 3) update test set value = 11 where id = 1; -- T1
        Put updateRow1Tx1 = new Put(rowId1);
        updateRow1Tx1.add(famName, colName, Bytes.toBytes("11"));
        txTable.put(tx1, updateRow1Tx1);

        // 4) update test set value = 11 where id = 1; -- T2
        Put updateRow1Tx2 = new Put(rowId1);
        updateRow1Tx2.add(famName, colName, Bytes.toBytes("11"));
        txTable.put(tx2, updateRow1Tx2);

        // 5) commit -- T1
        tm.commit(tx1);

        // 6) commit -- T2 --> should be rolled-back
        try {
            tm.commit(tx2);
            fail();
        } catch (RollbackException e) {
            // Expected
        }

    }

    @Test
    public void testSIPreventsReadSkew() throws Exception {
        // TX History for G-single:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where id = 1; -- T1. Shows 1 => 10
        // select * from test where id = 1; -- T2
        // select * from test where id = 2; -- T2
        // update test set value = 12 where id = 1; -- T2
        // update test set value = 18 where id = 2; -- T2
        // commit; -- T2
        // select * from test where id = 2; -- T1. Shows 2 => 20
        // commit; -- T1

        // 0) Start transactions
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        Scan rowId1Scan = new Scan(rowId1, rowId1);
        rowId1Scan.addColumn(famName, colName);

        // 1) select * from test where id = 1; -- T1. Shows 1 => 10
        ResultScanner tx1Scanner = txTable.getScanner(tx1, rowId1Scan);
        Result res = tx1Scanner.next();
        int count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                    Bytes.toString(res.getValue(famName, colName)));
            assertEquals(res.getRow(), rowId1);
            assertEquals(res.getValue(famName, colName), dataValue1);
            res = tx1Scanner.next();
            count++;
        }
        assertEquals(count, 1);

        // 2) select * from test where id = 1; -- T2
        ResultScanner tx2Scanner = txTable.getScanner(tx2, rowId1Scan);
        res = tx2Scanner.next();
        count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                    Bytes.toString(res.getValue(famName, colName)));
            assertEquals(res.getRow(), rowId1);
            assertEquals(res.getValue(famName, colName), dataValue1);
            res = tx2Scanner.next();
            count++;
        }

        Scan rowId2Scan = new Scan(rowId2, rowId2);
        rowId2Scan.addColumn(famName, colName);

        // 3) select * from test where id = 2; -- T2
        tx2Scanner = txTable.getScanner(tx2, rowId2Scan);
        res = tx2Scanner.next();
        count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                    Bytes.toString(res.getValue(famName, colName)));
            assertEquals(res.getRow(), rowId2);
            assertEquals(res.getValue(famName, colName), dataValue2);
            res = tx2Scanner.next();
            count++;
        }

        // 4) update test set value = 12 where id = 1; -- T2
        Put updateRow1Tx2 = new Put(rowId1);
        updateRow1Tx2.add(famName, colName, Bytes.toBytes("12"));
        txTable.put(tx1, updateRow1Tx2);

        // 5) update test set value = 18 where id = 1; -- T2
        Put updateRow2Tx2 = new Put(rowId2);
        updateRow2Tx2.add(famName, colName, Bytes.toBytes("18"));
        txTable.put(tx2, updateRow2Tx2);

        // 6) commit -- T2
        tm.commit(tx2);

        // 7) select * from test where id = 2; -- T1. Shows 2 => 20
        tx1Scanner = txTable.getScanner(tx1, rowId2Scan);
        res = tx1Scanner.next();
        count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                    Bytes.toString(res.getValue(famName, colName)));
            assertEquals(res.getRow(), rowId2);
            assertEquals(res.getValue(famName, colName), dataValue2);
            res = tx1Scanner.next();
            count++;
        }

        // 8) commit -- T1
        tm.commit(tx1);

    }

    @Test
    public void testSIPreventsReadSkewUsingWritePredicate() throws Exception {
        // TX History for G-single:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where id = 1; -- T1. Shows 1 => 10
        // select * from test; -- T2
        // update test set value = 12 where id = 1; -- T2
        // update test set value = 18 where id = 2; -- T2
        // commit; -- T2
        // delete from test where value = 20; -- T1. Prints "ERROR: could not serialize access due to concurrent update"
        // abort; -- T1. There's nothing else we can do, this transaction has failed

        // 0) Start transactions
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        Scan rowId1Scan = new Scan(rowId1, rowId1);
        rowId1Scan.addColumn(famName, colName);

        // 1) select * from test where id = 1; -- T1
        ResultScanner tx1Scanner = txTable.getScanner(tx1, rowId1Scan);
        Result res = tx1Scanner.next();
        int count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                    Bytes.toString(res.getValue(famName, colName)));
            assertEquals(res.getRow(), rowId1);
            assertEquals(res.getValue(famName, colName), dataValue1);
            res = tx1Scanner.next();
            count++;
        }
        assertEquals(count, 1);

        // 2) select * from test; -- T2
        Scan allRowsScan = new Scan();
        ResultScanner tx2Scanner = txTable.getScanner(tx2, allRowsScan);
        res = tx2Scanner.next();
        count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                    Bytes.toString(res.getValue(famName, colName)));
            res = tx2Scanner.next();
            count++;
        }
        assertEquals(count, 2);

        // 3) update test set value = 12 where id = 1; -- T2
        // 4) update test set value = 18 where id = 2; -- T2
        List<Put> putsForTx2 = new ArrayList<>();
        Put updateRow1Tx2 = new Put(rowId1);
        updateRow1Tx2.add(famName, colName, Bytes.toBytes("12"));
        Put updateRow2Tx2 = new Put(rowId2);
        updateRow2Tx2.add(famName, colName, Bytes.toBytes("18"));
        txTable.put(tx2, putsForTx2);

        // 5) commit; -- T2
        tm.commit(tx2);

        // 6) delete from test where value = 20; -- T1. Prints
        // "ERROR: could not serialize access due to concurrent update"
        Filter f = new SingleColumnValueFilter(famName, colName, CompareFilter.CompareOp.EQUAL, Bytes.toBytes("20"));
        Scan checkFor20 = new Scan();
        checkFor20.setFilter(f);
        ResultScanner checkFor20Scanner = txTable.getScanner(tx1, checkFor20);
        res = checkFor20Scanner.next();
        int count20 = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Deleting row id {} with value {}", Bytes.toString(res.getRow()),
                    Bytes.toString(res.getValue(famName, colName)));
            Delete delete20 = new Delete(res.getRow());
            txTable.delete(tx2, delete20);
            res = checkFor20Scanner.next();
            count20++;
        }

        // 7) abort; -- T1
        try {
            tm.commit(tx1);
            fail("Should be aborted");
        } catch (RollbackException e) {
            // Expected
        }

    }

    @Test
    public void testSIDoesNotPreventWriteSkew() throws Exception {
        // TX History for G2-item:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where id in (1,2); -- T1
        // select * from test where id in (1,2); -- T2
        // update test set value = 11 where id = 1; -- T1
        // update test set value = 21 where id = 2; -- T2
        // commit; -- T1
        // commit; -- T2

        // 0) Start transactions
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        Scan rowId12Scan = new Scan(rowId1, rowId3);
        rowId12Scan.addColumn(famName, colName);

        // 1) select * from test where id in (1,2); -- T1
        ResultScanner tx1Scanner = txTable.getScanner(tx1, rowId12Scan);
        Result res = tx1Scanner.next();
        int count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                    Bytes.toString(res.getValue(famName, colName)));
            switch (count) {
            case 0:
                assertEquals(res.getRow(), rowId1);
                assertEquals(res.getValue(famName, colName), dataValue1);
                break;
            case 1:
                assertEquals(res.getRow(), rowId2);
                assertEquals(res.getValue(famName, colName), dataValue2);
                break;
            default:
                fail();
            }
            res = tx1Scanner.next();
            count++;
        }
        assertEquals(count, 2);

        // 2) select * from test where id in (1,2); -- T2
        ResultScanner tx2Scanner = txTable.getScanner(tx1, rowId12Scan);
        res = tx2Scanner.next();
        count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                    Bytes.toString(res.getValue(famName, colName)));
            switch (count) {
            case 0:
                assertEquals(res.getRow(), rowId1);
                assertEquals(res.getValue(famName, colName), dataValue1);
                break;
            case 1:
                assertEquals(res.getRow(), rowId2);
                assertEquals(res.getValue(famName, colName), dataValue2);
                break;
            default:
                fail();
            }
            res = tx2Scanner.next();
            count++;
        }
        assertEquals(count, 2);

        // 3) update test set value = 11 where id = 1; -- T1
        Put updateRow1Tx1 = new Put(rowId1);
        updateRow1Tx1.add(famName, colName, Bytes.toBytes("11"));
        txTable.put(tx1, updateRow1Tx1);

        // 4) update test set value = 21 where id = 2; -- T2
        Put updateRow2Tx2 = new Put(rowId2);
        updateRow2Tx2.add(famName, colName, Bytes.toBytes("21"));
        txTable.put(tx2, updateRow2Tx2);

        // 5) commit; -- T1
        tm.commit(tx1);

        // 6) commit; -- T2
        tm.commit(tx2);
    }

    @Test
    public void testSIDoesNotPreventAntiDependencyCycles() throws Exception {
        // TX History for G2:
        // begin; set transaction isolation level repeatable read; -- T1
        // begin; set transaction isolation level repeatable read; -- T2
        // select * from test where value % 3 = 0; -- T1
        // select * from test where value % 3 = 0; -- T2
        // insert into test (id, value) values(3, 30); -- T1
        // insert into test (id, value) values(4, 42); -- T2
        // commit; -- T1
        // commit; -- T2
        // select * from test where value % 3 = 0; -- Either. Returns 3 => 30, 4 => 42

        // 0) Start transactions
        Transaction tx1 = tm.begin();
        Transaction tx2 = tm.begin();

        Filter f = new SingleColumnValueFilter(famName, colName, CompareFilter.CompareOp.EQUAL, Bytes.toBytes("30"));
        Scan value30 = new Scan();
        value30.setFilter(f);
        value30.addColumn(famName, colName);

        // 1) select * from test where value % 3 = 0; -- T1
        ResultScanner tx1Scanner = txTable.getScanner(tx1, value30);
        Result res = tx1Scanner.next();
        int count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                    Bytes.toString(res.getValue(famName, colName)));
            res = tx1Scanner.next();
            count++;
        }
        assertEquals(count, 0);

        // 2) select * from test where value % 3 = 0; -- T2
        ResultScanner tx2Scanner = txTable.getScanner(tx1, value30);
        res = tx2Scanner.next();
        count = 0;
        while (res != null) {
            LOG.info("RESSS {}", res);
            LOG.info("Row id {} with value {}", Bytes.toString(res.getRow()),
                    Bytes.toString(res.getValue(famName, colName)));
            res = tx2Scanner.next();
            count++;
        }
        assertEquals(count, 0);

        // 3) insert into test (id, value) values(3, 30); -- T1
        Put insertRow3Tx1 = new Put(rowId1);
        insertRow3Tx1.add(famName, colName, Bytes.toBytes("30"));
        txTable.put(tx1, insertRow3Tx1);

        // 4) insert into test (id, value) values(4, 42); -- T2
        Put updateRow4Tx2 = new Put(rowId2);
        updateRow4Tx2.add(famName, colName, Bytes.toBytes("42"));
        txTable.put(tx2, updateRow4Tx2);

        // 5) commit; -- T1
        tm.commit(tx1);

        // 6) commit; -- T2
        tm.commit(tx2);

        // 7) select * from test where value % 3 = 0; -- Either. Returns 3 => 30, 4 => 42
    }

    // ************************************************************************

    private static String TEST_TSO_HOST = "localhost";
    private static int TEST_TSO_PORT = 1234;

    protected static final String TEST_TABLE = "baillis-anomalies-test-table";
    protected static final String TEST_FAMILY = "baillis-cf";
    protected static final String TEST_COLUMN = "baillis-col";
    protected static final TableName TABLE_NAME = TableName.valueOf(TEST_TABLE);

    private Injector injector = null;

    private TSOServer tso;
    private TSOClient tsoClient;

    protected static HBaseTestingUtility testutil;
    private static MiniHBaseCluster hbasecluster;
    protected static org.apache.hadoop.conf.Configuration hbaseConf;

    private CommitTable commitTable;
    private CommitTable.Client commitTableClient = null;

    protected TransactionManager newTransactionManager() throws Exception {
        return HBaseTransactionManager.newBuilder()
                .withConfiguration(hbaseConf)
                .withCommitTableClient(commitTableClient)
                .withTSOClient(tsoClient).build();
    }

    @BeforeClass
    public void setupInfrastructure() throws Exception {
        injector = Guice
                .createInjector(new TSOMockModule(TSOServerCommandLineConfig.configFactory(TEST_TSO_PORT, 1000)));
        setupTSOServer();
        setupTSOClient();
        setupHBaseCluster();
    }

    private void setupTSOServer() throws UnknownHostException, IOException, InterruptedException {
        LOG.info("Starting TSO Server");
        tso = injector.getInstance(TSOServer.class);
        tso.startAndWait();
        TestUtils.waitForSocketListening(TEST_TSO_HOST, TEST_TSO_PORT, 100);
        LOG.info("Finished loading TSO Server");
    }

    private void setupTSOClient() throws Exception {

        Configuration clientConf = new BaseConfiguration();
        clientConf.setProperty(TSO_HOST_CONFKEY, TEST_TSO_HOST);
        clientConf.setProperty(TSO_PORT_CONFKEY, TEST_TSO_PORT);

        commitTable = injector.getInstance(CommitTable.class);
        commitTableClient = commitTable.getClient().get();

        // Create the associated Handler
        tsoClient = TSOClient.newBuilder().withConfiguration(clientConf)
                .build();

    }

    private void setupHBaseCluster() throws Exception {
        // HBase setup
        hbaseConf = HBaseConfiguration.create();
        hbaseConf.setInt("hbase.hregion.memstore.flush.size", 100 * 1024);
        hbaseConf.setInt("hbase.regionserver.nbreservationblocks", 1);
        hbaseConf.setInt(HBASE_CLIENT_RETRIES_NUMBER, 3);
        hbaseConf.set(TSO_HOST_CONFKEY, TEST_TSO_HOST);
        hbaseConf.setInt(TSO_PORT_CONFKEY, TEST_TSO_PORT);

        final String rootdir = "/tmp/hbase.test.dir/";
        File rootdirFile = new File(rootdir);
        if (rootdirFile.exists()) {
            delete(rootdirFile);
        }
        hbaseConf.set("hbase.rootdir", rootdir);

        LOG.info("Starting HBase Cluster");
        testutil = new HBaseTestingUtility(hbaseConf);
        hbasecluster = testutil.startMiniCluster(5);
        LOG.info("HBase Cluster stopped");
    }

    private void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    @AfterClass
    public void teardownInfrastructure() throws Exception {
        LOG.info("Tearing down infrastructure");
        testutil.shutdownMiniCluster(); // Stop HBase cluster
        teardownTSOClient();
        teardownTSOServer();
    }

    private void teardownTSOClient() throws Exception {
        tsoClient.close().get();
    }

    public void teardownTSOServer() throws Exception {
        tso.stopAndWait();
        tso = null;
        TestUtils.waitForSocketNotListening(TEST_TSO_HOST, TEST_TSO_PORT, 1000);
    }

    @BeforeMethod
    public void setUpExperiment() throws Exception {
        HBaseAdmin admin = testutil.getHBaseAdmin();

        if (!admin.tableExists(TEST_TABLE)) {
            HTableDescriptor desc = new HTableDescriptor(TABLE_NAME);
            HColumnDescriptor datafam = new HColumnDescriptor(TEST_FAMILY);
            datafam.setMaxVersions(Integer.MAX_VALUE);
            desc.addFamily(datafam);
            admin.createTable(desc);
        }

        if (admin.isTableDisabled(TEST_TABLE)) {
            admin.enableTable(TEST_TABLE);
        }

        HTableDescriptor[] tables = admin.listTables();
        LOG.info("List of tables:");
        for (HTableDescriptor t : tables) {
            LOG.info("\tTable -> {}", t.getNameAsString());
        }

        CreateTable.createTable(hbaseConf, HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME, 1);

        initializeTransactionalStuff();
        loadBaseDataOnTestTable();
    }

    private void initializeTransactionalStuff() throws Exception, IOException {
        tm = newTransactionManager();
        txTable = new TTable(hbaseConf, TEST_TABLE);
    }

    /**
     * This translates the table initialization done in:
     * https://github.com/ept/hermitage/blob/master/postgres.md
     *
     * create table test (id int primary key, value int);
     * insert into test (id, value) values (1, 10), (2, 20);
     */
    private void loadBaseDataOnTestTable() throws TransactionException, IOException, RollbackException {

        Transaction initializationTx = tm.begin();

        Put row1 = new Put(rowId1);
        row1.add(famName, colName, dataValue1);
        txTable.put(initializationTx, row1);
        Put row2 = new Put(rowId2);
        row2.add(famName, colName, dataValue2);
        txTable.put(initializationTx, row2);

        tm.commit(initializationTx);
    }

    @AfterMethod
    public void tearDownExperiment() {
        try {
            LOG.info("Tearing Down Experiment");

            tearDownTransactionalStuff();

            HBaseAdmin admin = testutil.getHBaseAdmin();
            admin.disableTable(TEST_TABLE);
            admin.deleteTable(TEST_TABLE);

            admin.disableTable(HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME);
            admin.deleteTable(HBaseCommitTable.COMMIT_TABLE_DEFAULT_NAME);
        } catch (Exception e) {
            LOG.error("Error tearing down", e);
        }
    }

    private void tearDownTransactionalStuff() throws Exception, IOException {
        txTable.close();
        tm.close();
    }

}
