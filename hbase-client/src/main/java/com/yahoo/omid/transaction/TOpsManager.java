package com.yahoo.omid.transaction;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

/**
 * Transactional operations through Omid
 */
public class TOpsManager implements Closeable {

    private static final int MAX_CACHED_TABLES = 100;

    private final LoadingCache<String, TTable> tableCache;

    private final TransactionManager tm;

    private final boolean ownsTransactionManager = true;

    private volatile boolean isClosing;

    // ////////////////////////////////////////////////////////////////////////
    // Instantiation
    // ////////////////////////////////////////////////////////////////////////

    // Avoid public instantiation
    private TOpsManager(final Builder builder) {
        this.tableCache = builder.tableCache;
        this.tm = builder.tm;
        this.isClosing = false;
    }

    // Preferred method to instantiate
    public static Builder newBuilder(Configuration conf) throws OmidInstantiationException {
        return new Builder(conf);
    }

    public static final class Builder {

        final Configuration conf;

        LoadingCache<String, TTable> tableCache;

        TransactionManager tm;

        private CacheLoader<String, TTable> defaultCacheLoader = new CacheLoader<String, TTable>() {
            @Override
            public TTable load(String tableName) throws Exception {
                try (HBaseAdmin admin = new HBaseAdmin(conf)) {
                    if (admin.tableExists(tableName))
                        return new TTable(conf, tableName);
                    throw new IOException("Table " + tableName + "does not exists in HBase");
                }
            }
        };


        Builder(Configuration conf) throws OmidInstantiationException {
            this.conf = conf;
            this.tableCache = CacheBuilder.newBuilder()
                                          .maximumSize(MAX_CACHED_TABLES)
                                          .build(defaultCacheLoader);

            this.tm = HBaseTransactionManager.newBuilder()
                                             .withConfiguration(conf)
                                             .build();
        }

        @VisibleForTesting
        Builder tableCache(LoadingCache<String, TTable> cache) {
            checkState(tableCache == null, "tableCache already set to  %s", tableCache);
            tableCache = checkNotNull(cache);
            return this;
        }

        @VisibleForTesting
        Builder transactionManager(TransactionManager txMgr) {
            checkState(tm == null, "transactionManager already set to  %s", tm);
            tm = checkNotNull(txMgr);
            return this;
        }

        public TOpsManager build() {
            return new TOpsManager(this);
        }
    }

    // ////////////////////////////////////////////////////////////////////////
    // Closeable interface
    // ////////////////////////////////////////////////////////////////////////

    @Override
    public void close() throws IOException {
        isClosing = true;

        ConcurrentMap<String, TTable> cacheAsMap = tableCache.asMap();
        for (TTable tTable : cacheAsMap.values()) {
            tTable.close();
        }
        tableCache.invalidateAll();
        tableCache.cleanUp();
        if (ownsTransactionManager) { // TODO configure on creation if required
            tm.close();
        }
    }

    // ////////////////////////////////////////////////////////////////////////
    // Transactional Operations
    // ////////////////////////////////////////////////////////////////////////

    /**
     * Transactional get
     */
    public Result get(final byte[] tableName, final Get get) throws TransactionException {
        return get(Bytes.toString(tableName), get);
    }

    public Result get(final String tableName, final Get get) throws TransactionException {

        checkState(!isClosing, "This Manager for Tx ops is closed");

        TTable txTable = getTTableFromCache(tableName);
        Result result;
        Transaction tx = tm.begin();
        try {
            result = txTable.get(tx, get);
            tm.commit(tx);
        } catch (IOException | RollbackException /* should not occur */e) {
            tm.rollback(tx);
            String msg = "There was a problem executing transactional get.\n" +
                         "(Tx: " + tx + "/ Get: " + get + ")";
            throw new TransactionException(msg, e.getCause());
        }
        return result;

    }

    // ////////////////////////////////////////////////////////////////////////
    // Helper methods
    // ////////////////////////////////////////////////////////////////////////

    @VisibleForTesting
    TTable getTTableFromCache(final String tableName) throws TransactionException {
        try {
            return tableCache.get(tableName);
        } catch (ExecutionException e) {
            String msg = "Can't get transactional table " + tableName;
            throw new TransactionException(msg, e.getCause());
        }
    }

}
