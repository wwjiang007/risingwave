package com.risingwave.connector;

import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.util.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;

public class FunctionInitializationContextImpl implements FunctionInitializationContext {
    private long checkpointId;
    private OperatorStateStore operatorStateStore;

    public FunctionInitializationContextImpl(long checkpointId , OperatorStateStore operatorStateStore){
        this.checkpointId = checkpointId;
        this.operatorStateStore = operatorStateStore;
    }

    @Override
    public OptionalLong getRestoredCheckpointId() {
        return OptionalLong.of(checkpointId);
    }

    @Override
    public OperatorStateStore getOperatorStateStore() {
        return operatorStateStore;
    }

    @Override
    public KeyedStateStore getKeyedStateStore() {
        throw new NotSupportException();
    }

    static class ListStateImpl<T> implements ListState<T> {

        public final List<T> saveList = new ArrayList<>();
        private boolean clearCalled = false;

        @Override
        public void update(List<T> ts) throws Exception {
            clear();

            addAll(ts);
        }

        @Override
        public void addAll(List<T> ts) throws Exception {
            for (T t:ts) {
                add(t);
            }
        }

        @Override
        public List<T> get() throws Exception {
            return saveList;
        }

        @Override
        public void add(T t) throws Exception {
            Preconditions.checkNotNull(t);
            saveList.add(t);
        }

        @Override
        public void clear() {
            this.saveList.clear();
            this.clearCalled = true;
        }
    }
}
