package com.yahoo.omid.transaction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventTranslatorTwoArg;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.yahoo.omid.transaction.HBaseAsyncSyncShadowCellsUpdater.UpdateShadowCellsEvent;

public class HBaseAsyncSyncShadowCellsUpdater
        extends HBaseSyncShadowCellsUpdater
{

    private static final Logger LOG = LoggerFactory.getLogger(HBaseAsyncSyncShadowCellsUpdater.class);

    private static final int NUM_SHADOW_CELLS_UPDATERS = 2;

    private final RingBuffer<UpdateShadowCellsEvent> updateRequestRing;

    public HBaseAsyncSyncShadowCellsUpdater() {

        ExecutorService shadowCellsUpdaterExecutor = Executors.newFixedThreadPool(
                NUM_SHADOW_CELLS_UPDATERS,
                new ThreadFactoryBuilder().setNameFormat("shadowCellsUpdater-%d").build());

        // Set up the disruptor thread
        updateRequestRing = RingBuffer.<UpdateShadowCellsEvent> createMultiProducer(
                UpdateShadowCellsEvent.FACTORY, 1 << 12 /* 2^12 = 4096 */,
                new BusySpinWaitStrategy());
        SequenceBarrier sequenceBarrier = updateRequestRing.newBarrier();

        for (int i = 0; i < NUM_SHADOW_CELLS_UPDATERS; i++) {
            BatchEventProcessor<UpdateShadowCellsEvent> shadowCellsUpdaterProcessor =
                    new BatchEventProcessor<UpdateShadowCellsEvent>(updateRequestRing,
                            sequenceBarrier, new UpdateShadowCellsEventHandler(this));
            updateRequestRing.addGatingSequences(shadowCellsUpdaterProcessor.getSequence());
            // shadowCellsUpdaterProcessor.setExceptionHandler(new FatalExceptionHandler(panicker));

            // Each processor runs on a separate thread
            shadowCellsUpdaterExecutor.submit(shadowCellsUpdaterProcessor);
        }
    }

    // ////////////////////////////////////////////////////////////////////////
    // ShadowCellsUpdater implementation
    // ////////////////////////////////////////////////////////////////////////

    private static final EventTranslatorTwoArg<UpdateShadowCellsEvent, HBaseTransaction, SettableFuture<Void>> TRANSLATOR =
            new EventTranslatorTwoArg<UpdateShadowCellsEvent, HBaseTransaction, SettableFuture<Void>>() {

                @Override
                public void translateTo(UpdateShadowCellsEvent event, long sequence,
                                        HBaseTransaction tx,
                                        SettableFuture<Void> f)
                {
                    UpdateShadowCellsEvent.configureUpdateShadowCellRequest(event, tx, f);
                }

            };

    @Override
    public ListenableFuture<Void> asyncUpdate(HBaseTransaction tx) {
        LOG.info("Event for tx {}", tx);
        SettableFuture<Void> f = SettableFuture.<Void>create();
        updateRequestRing.publishEvent(TRANSLATOR, tx, f);
        return f;
    }

    // ////////////////////////////////////////////////////////////////////////
    // EventHandler implementation
    // ////////////////////////////////////////////////////////////////////////
    static class UpdateShadowCellsEventHandler implements EventHandler<UpdateShadowCellsEvent> {

        private ShadowCellsUpdater shadowCellsUpdater;

        public UpdateShadowCellsEventHandler(ShadowCellsUpdater shadowCellsUpdater) {
            this.shadowCellsUpdater = shadowCellsUpdater;
        }

        @Override
        public void onEvent(UpdateShadowCellsEvent event, long sequence, boolean endOfBatch)
                throws Exception {
            try {
                LOG.info("Processing event for tx {}", event.getTransaction());
                shadowCellsUpdater.update(event.getTransaction());
                event.getFuture().set(null);
            } catch (TransactionManagerException tme) {
                event.getFuture().setException(tme);
            }
        }
    }

    // ////////////////////////////////////////////////////////////////////////
    // Events
    // ////////////////////////////////////////////////////////////////////////
    final static class UpdateShadowCellsEvent {

        private HBaseTransaction tx;
        private SettableFuture<Void> future;

        static void configureUpdateShadowCellRequest(UpdateShadowCellsEvent e,
                                                     HBaseTransaction tx,
                                                     SettableFuture<Void> f) {
            e.tx = tx;
            e.future = f;
        }

        HBaseTransaction getTransaction() {
            return tx;
        }

        SettableFuture<Void> getFuture() {
            return future;
        }

        public final static EventFactory<UpdateShadowCellsEvent> FACTORY =
                new EventFactory<UpdateShadowCellsEvent>() {
            @Override
            public UpdateShadowCellsEvent newInstance()
            {
                return new UpdateShadowCellsEvent();
            }
        };
    }

}
