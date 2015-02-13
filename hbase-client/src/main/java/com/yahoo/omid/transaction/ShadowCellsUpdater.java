package com.yahoo.omid.transaction;

import com.google.common.util.concurrent.ListenableFuture;

public interface ShadowCellsUpdater {

    public ListenableFuture<Void> asyncUpdate(HBaseTransaction tx);

    public void update(HBaseTransaction tx) throws TransactionManagerException;

}
