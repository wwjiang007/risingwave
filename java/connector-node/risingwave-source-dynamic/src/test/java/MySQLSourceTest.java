import com.risingwave.connector.FlinkRuntimeContextImpl;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.CheckedThread;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.*;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.util.Preconditions;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class MySQLSourceTest {

        @Test
        public void testLiness() throws Exception {

            ObjectIdentifier objectIdentifier = ObjectIdentifier.of("","","");
            List<Column> flinkColumns = new ArrayList<>();
            List<String> flinkPkColumns = new ArrayList<>();
            flinkColumns.add(Column.physical("v1", DataTypes.INT()));
            flinkColumns.add(Column.physical("v2", DataTypes.INT()));
            flinkColumns.add(Column.physical("v3", DataTypes.INT()));
            flinkPkColumns.add("v1");
            ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
            DynamicTableSourceFactory dynamicTableSourceFactory =
                    FactoryUtil.discoverFactory(
                            contextClassLoader, DynamicTableSourceFactory.class, "mysql-cdc");
            Schema.Builder schemaBuilder = Schema.newBuilder();

            for (Column columnDesc : flinkColumns) {
                schemaBuilder.column(
                        columnDesc.getName(), columnDesc.getDataType().toString());
            }

            Map<String,String> map = new HashMap<>();
            map.put("hostname","127.0.0.1");
            map.put("port","3306");
            map.put("username","users");
            map.put("password","123456");
            map.put("database-name","demo");
            map.put("table-name","test");
            map.put("scan.incremental.snapshot.enabled","false");

            Schema shcema = schemaBuilder.primaryKey("v1").build();
            ResolvedCatalogTable resolvedCatalogTable =
                    new ResolvedCatalogTable(
                            CatalogTable.of(
                                    shcema, null, shcema.getPrimaryKey().get().getColumnNames(), map),
                            new ResolvedSchema(flinkColumns,Collections.emptyList(),UniqueConstraint.primaryKey("1",flinkPkColumns)));
            FactoryUtil.DefaultDynamicTableContext defaultDynamicTableContext = new FactoryUtil.DefaultDynamicTableContext(
                    objectIdentifier,
                    resolvedCatalogTable,
                    new HashMap<String, String>(),
                    new Configuration(),
                    contextClassLoader,
                    false);
            ScanTableSource dynamicTableSource = (ScanTableSource) dynamicTableSourceFactory.createDynamicTableSource(defaultDynamicTableContext);
            SourceFunctionProvider scanRuntimeProvider = (SourceFunctionProvider) dynamicTableSource.getScanRuntimeProvider(new ScanRuntimeProviderContext());
            DebeziumSourceFunction sourceFunction = (DebeziumSourceFunction) scanRuntimeProvider.createSourceFunction();
            sourceFunction.setRuntimeContext(new FlinkRuntimeContextImpl());
            FunctionInitializationContext functionInitializationContext = new FunctionInitializationContext() {
                final TestingListState<byte[]> offsetState = new TestingListState<>();
                final TestingListState<String> historyState = new TestingListState<>();
                @Override
                public OptionalLong getRestoredCheckpointId() {
                    return OptionalLong.of(1);
                }

                @Override
                public OperatorStateStore getOperatorStateStore() {
                    return new MockOperatorStateStore(offsetState, historyState);
                }

                final class TestingListState<T> implements ListState<T> {

                    public final List<T> list = new ArrayList<>();
                    private boolean clearCalled = false;

                    @Override
                    public void clear() {
                        list.clear();
                        clearCalled = true;
                    }

                    @Override
                    public Iterable<T> get() {
                        return list;
                    }

                    @Override
                    public void add(T value) {
                        Preconditions.checkNotNull(value, "You cannot add null to a ListState.");
                        list.add(value);
                    }

                    public List<T> getList() {
                        return list;
                    }

                    boolean isClearCalled() {
                        return clearCalled;
                    }

                    @Override
                    public void update(List<T> values) {
                        clear();

                        addAll(values);
                    }

                    @Override
                    public void addAll(List<T> values) {
                        if (values != null) {
                            values.forEach(
                                    v -> Preconditions.checkNotNull(v, "You cannot add null to a ListState."));

                            list.addAll(values);
                        }
                    }
                }

                @Override
                public KeyedStateStore getKeyedStateStore() {
                    return null;
                }
            };
            sourceFunction.initializeState(functionInitializationContext);

            sourceFunction.open(new Configuration());
            TestSourceContext<SourceRecord> objectTestSourceContext = new TestSourceContext<>();
            final CheckedThread runThread3 =
                    new CheckedThread() {
                        @Override
                        public void go() throws Exception {
                            sourceFunction.run(objectTestSourceContext);
                        }
                    };
            runThread3.start();

            List<SourceRecord> allRecords = new ArrayList<>();
            LinkedBlockingQueue<StreamRecord<SourceRecord>> queue = objectTestSourceContext.getCollectedOutputs();
            int a = 1;
            while (a < 7) {
                StreamRecord<SourceRecord> record = null;
                try {
                    record = queue.poll(1000, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                if (record != null) {
                    System.out.println(record);
                } else {
                    throw new RuntimeException(
                            "Can't receive  elements before timeout.");
                }
                a++;
            }
            synchronized (objectTestSourceContext.getCheckpointLock()){
                sourceFunction.snapshotState(new FunctionSnapshotContext() {
                    @Override
                    public long getCheckpointId() {
                        return 101;
                    }

                    @Override
                    public long getCheckpointTimestamp() {
                        return 0;
                    }
                });
            }

            System.out.println(1);
            Thread.sleep(1000000000);

        }
    public class TestSourceContext<T> implements SourceFunction.SourceContext<T> {

        private final Object checkpointLock = new Object();

        private LinkedBlockingQueue<StreamRecord<T>> collectedOutputs = new LinkedBlockingQueue<>();

        @Override
        public void collect(T element) {
            RowData element1 = (RowData) element;
            System.out.println(element1);
            this.collectedOutputs.add(new StreamRecord<>(element));
        }

        @Override
        public void collectWithTimestamp(T element, long timestamp) {
            this.collectedOutputs.offer(new StreamRecord<>(element, timestamp));
        }

        @Override
        public void emitWatermark(Watermark mark) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void markAsTemporarilyIdle() {}

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }

        @Override
        public void close() {}

        public StreamRecord<T> removeLatestOutput() {
            return collectedOutputs.poll();
        }

        public LinkedBlockingQueue<StreamRecord<T>>  getCollectedOutputs() {
            return collectedOutputs;
        }
    }

        private static class MockOperatorStateStore implements OperatorStateStore {

            private final ListState<?> restoredOffsetListState;
            private final ListState<?> restoredHistoryListState;

            private MockOperatorStateStore(
                    ListState<?> restoredOffsetListState, ListState<?> restoredHistoryListState) {
                this.restoredOffsetListState = restoredOffsetListState;
                this.restoredHistoryListState = restoredHistoryListState;
            }

            @Override
            @SuppressWarnings("unchecked")
            public <S> ListState<S> getUnionListState(ListStateDescriptor<S> stateDescriptor) {
                if (stateDescriptor.getName().equals(DebeziumSourceFunction.OFFSETS_STATE_NAME)) {
                    return (ListState<S>) restoredOffsetListState;
                } else if (stateDescriptor
                        .getName()
                        .equals(DebeziumSourceFunction.HISTORY_RECORDS_STATE_NAME)) {
                    return (ListState<S>) restoredHistoryListState;
                } else {
                    throw new IllegalStateException("Unknown state.");
                }
            }

            @Override
            public <K, V> BroadcastState<K, V> getBroadcastState(
                    MapStateDescriptor<K, V> stateDescriptor) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <S> ListState<S> getListState(ListStateDescriptor<S> stateDescriptor) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Set<String> getRegisteredStateNames() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Set<String> getRegisteredBroadcastStateNames() {
                throw new UnsupportedOperationException();
            }
        }
}
