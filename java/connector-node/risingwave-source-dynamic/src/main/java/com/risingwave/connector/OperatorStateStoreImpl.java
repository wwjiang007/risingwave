package com.risingwave.connector;

import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.state.*;

import java.util.Set;

public class OperatorStateStoreImpl implements OperatorStateStore {

    private final ListState<?> restoredOffsetListState;

    private final ListState<?> restoredHistoryListState;

    public OperatorStateStoreImpl(){
        this.restoredHistoryListState = new FunctionInitializationContextImpl.ListStateImpl();
        this.restoredOffsetListState = new FunctionInitializationContextImpl.ListStateImpl();
    }

    @Override
    public <K, V> BroadcastState<K, V> getBroadcastState(MapStateDescriptor<K, V> mapStateDescriptor) throws Exception {
        throw new NotSupportException();
    }

    @Override
    public <S> ListState<S> getListState(ListStateDescriptor<S> listStateDescriptor) throws Exception {
        throw new NotSupportException();
    }

    @Override
    public <S> ListState<S> getUnionListState(ListStateDescriptor<S> listStateDescriptor) throws Exception {
        if (listStateDescriptor.getName().equals(DebeziumSourceFunction.OFFSETS_STATE_NAME)) {
            return (ListState<S>) restoredOffsetListState;
        } else if (listStateDescriptor
                .getName()
                .equals(DebeziumSourceFunction.HISTORY_RECORDS_STATE_NAME)) {
            return (ListState<S>) restoredHistoryListState;
        } else {
            throw new IllegalStateException("Unknown state.");
        }
    }

    @Override
    public Set<String> getRegisteredStateNames() {
        throw new NotSupportException();
    }

    @Override
    public Set<String> getRegisteredBroadcastStateNames() {
        throw new NotSupportException();
    }
}
