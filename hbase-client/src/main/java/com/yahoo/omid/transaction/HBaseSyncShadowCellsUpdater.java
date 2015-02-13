package com.yahoo.omid.transaction;

import java.io.IOException;
import java.util.Set;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;

public class HBaseSyncShadowCellsUpdater implements ShadowCellsUpdater {

    private static final Logger LOG = LoggerFactory.getLogger(HBaseSyncShadowCellsUpdater.class);

    public HBaseSyncShadowCellsUpdater() {
    }

    // ////////////////////////////////////////////////////////////////////////
    // ShadowCellsUpdater implementation
    // ////////////////////////////////////////////////////////////////////////

    @Override
    public ListenableFuture<Void> asyncUpdate(HBaseTransaction tx) {
        throw new UnsupportedOperationException("This is a synchronous updater");
    }

    @Override
    public void update(HBaseTransaction tx)
            throws TransactionManagerException {

        Set<HBaseCellId> cells = tx.getWriteSet();

        // Add shadow cells
        for (HBaseCellId cell : cells) {
            Put put = new Put(cell.getRow());
            put.add(cell.getFamily(),
                    CellUtils.addShadowCellSuffix(cell.getQualifier(), 0, cell.getQualifier().length),
                    tx.getStartTimestamp(),
                    Bytes.toBytes(tx.getCommitTimestamp()));
            try {
                cell.getTable().put(put);
            } catch (IOException e) {
                throw new TransactionManagerException(
                        "Failed inserting shadow cell " + cell + " for Tx " + tx, e);
            }
        }
    }

}
