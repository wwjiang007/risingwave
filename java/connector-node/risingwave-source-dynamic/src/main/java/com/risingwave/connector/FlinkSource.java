package com.risingwave.connector;

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.util.functions.StreamingFunctionUtils;
import org.apache.flink.table.factories.FactoryUtil;

public interface FlinkSource<T> {
    public void init(FunctionInitializationContextImpl functionInitializationContext);
    public void open(SourceFunction.SourceContext<T> sourceContext);
    public void close();

    public void saveStates(long checkpointId);

    public void notifyCheckpointComplete(long checkpointId);
    public void notifyCheckpointAbort(long checkpointId);
}

class SourceFunctionImpl<T> implements FlinkSource<T>{
    SourceFunction<T> sourceFunction;
    public SourceFunctionImpl(SourceFunction<T> sourceFunction){
        this.sourceFunction = sourceFunction;
    }
    @Override
    public void init(FunctionInitializationContextImpl functionInitializationContext) {
        try {
            FunctionUtils.setFunctionRuntimeContext(sourceFunction, new FlinkRuntimeContextImpl());
            StreamingFunctionUtils.restoreFunctionState((StateInitializationContext) functionInitializationContext, sourceFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void open(SourceFunction.SourceContext<T> sourceContext) {
        try {
            FunctionUtils.openFunction(sourceFunction, new Configuration());
            sourceFunction.run(sourceContext);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            FunctionUtils.closeFunction(sourceFunction);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void saveStates(long checkpointId) {
        if( sourceFunction instanceof CheckpointedFunction){
            CheckpointedFunction sourceFunction1 = (CheckpointedFunction) sourceFunction;
            try {
                sourceFunction1.snapshotState(new FunctionSnapshotContext() {
                    @Override
                    public long getCheckpointId() {
                        return checkpointId;
                    }

                    @Override
                    public long getCheckpointTimestamp() {
                        return System.currentTimeMillis();
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }else{
            throw new NotSupportException();
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if( sourceFunction instanceof CheckpointListener){
            CheckpointListener sourceFunction1 = (CheckpointListener) sourceFunction;
            try {
                sourceFunction1.notifyCheckpointComplete(checkpointId);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }else{
            throw new NotSupportException();
        }
    }

    @Override
    public void notifyCheckpointAbort(long checkpointId) {
        if( sourceFunction instanceof CheckpointListener){
            CheckpointListener sourceFunction1 = (CheckpointListener) sourceFunction;
            try {
                sourceFunction1.notifyCheckpointAborted(checkpointId);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }else{
            throw new NotSupportException();
        }
    }
}
