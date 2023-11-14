package com.risingwave.connector;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.*;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.GenericMetricGroup;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

public class FlinkRuntimeContextImpl implements RuntimeContext {
    @Override
    public JobID getJobId() {
        return new JobID();
    }

    @Override
    public String getTaskName() {
        return "rw";
    }

    @Override
    public OperatorMetricGroup getMetricGroup() {
       return InternalSourceReaderMetricGroup.mock((new GenericMetricGroup(new NoOpMetricRegistry(), null, "rootMetricGroup")));
    }

    @Override
    public int getNumberOfParallelSubtasks() {
        return 0;
    }

    @Override
    public int getMaxNumberOfParallelSubtasks() {
        return 0;
    }

    @Override
    public int getIndexOfThisSubtask() {
        return 0;
    }

    @Override
    public int getAttemptNumber() {
        return 0;
    }

    @Override
    public String getTaskNameWithSubtasks() {
        return null;
    }

    @Override
    public ExecutionConfig getExecutionConfig() {
        return null;
    }

    @Override
    public ClassLoader getUserCodeClassLoader() {
        return null;
    }

    @Override
    public void registerUserCodeClassLoaderReleaseHookIfAbsent(String releaseHookName, Runnable releaseHook) {

    }

    @Override
    public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) {

    }

    @Override
    public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) {
        return null;
    }

    @Override
    public IntCounter getIntCounter(String name) {
        return null;
    }

    @Override
    public LongCounter getLongCounter(String name) {
        return null;
    }

    @Override
    public DoubleCounter getDoubleCounter(String name) {
        return null;
    }

    @Override
    public Histogram getHistogram(String name) {
        return null;
    }

    @Override
    public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) {
        return null;
    }

    @Override
    public boolean hasBroadcastVariable(String name) {
        return false;
    }

    @Override
    public <RT> List<RT> getBroadcastVariable(String name) {
        return null;
    }

    @Override
    public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) {
        return null;
    }

    @Override
    public DistributedCache getDistributedCache() {
        return null;
    }

    @Override
    public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
        return null;
    }

    @Override
    public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) {
        return null;
    }

    @Override
    public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) {
        return null;
    }

    @Override
    public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) {
        return null;
    }

    @Override
    public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
        return null;
    }
}
