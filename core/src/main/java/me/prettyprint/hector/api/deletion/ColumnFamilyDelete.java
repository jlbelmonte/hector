package me.prettyprint.hector.api.deletion;


import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.*;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.RangeSlicesCounterQuery;
import me.prettyprint.hector.api.query.RangeSlicesQuery;
import me.prettyprint.hector.api.query.SliceCounterQuery;

import java.nio.ByteBuffer;
import java.util.List;

public class ColumnFamilyDelete {

    public static void clearColumnFamily(Keyspace ksp, String columnFamilyName) {
        ByteBufferSerializer bs = ByteBufferSerializer.get();
        StringSerializer ss = StringSerializer.get();

        RangeSlicesQuery<ByteBuffer, String, ByteBuffer> query = HFactory.createRangeSlicesQuery(ksp, bs, ss, bs);

        ByteBuffer lastKey = null;
        boolean finished = false;
        while (!finished) {
            query.setColumnFamily(columnFamilyName);
            query.setKeys(lastKey, null);
            query.setReturnKeysOnly();
            query.setRowCount(11);
            OrderedRows<ByteBuffer, String, ByteBuffer> orderedRows = query.execute().get();
            List<Row<ByteBuffer, String, ByteBuffer>> rows;
            if (orderedRows == null || orderedRows.getCount() == 0 || orderedRows.getCount() < 11) {
                finished = true;
                rows = orderedRows.getList();
            } else {
                lastKey = orderedRows.peekLast().getKey();
                rows = orderedRows.getList().subList(0, orderedRows.getCount() - 1);
            }
            Mutator<ByteBuffer> mutator = HFactory.createMutator(ksp, ByteBufferSerializer.get());
            for (Row<ByteBuffer, String, ByteBuffer> row : rows) {
                mutator.delete(row.getKey(), columnFamilyName, null, bs);
                mutator.execute();
            }
        }
    }

    public static void clearCounterColumnFamily(Keyspace ksp, String counterColumnFamilyName) {
        ByteBufferSerializer bs = ByteBufferSerializer.get();
        StringSerializer ss = StringSerializer.get();

        RangeSlicesCounterQuery<ByteBuffer, ByteBuffer> query = HFactory.createRangeSlicesCounterQuery(ksp, bs, bs);
        boolean finished = false;
        ByteBuffer lastKey = null;
        while (!finished) {
            query.setColumnFamily(counterColumnFamilyName);
            query.setKeys(lastKey, null);
            query.setRowCount(11);
            query.setReturnKeysOnly();
            OrderedCounterRows<ByteBuffer, ByteBuffer> orderedRows = query.execute().get();
            List<CounterRow<ByteBuffer, ByteBuffer>> rows;
            if (orderedRows == null || orderedRows.getCount() == 0 || orderedRows.getCount() < 11) {
                finished = true;
                rows = orderedRows.getList();
            } else {
                lastKey = orderedRows.peekLast().getKey();
                rows = orderedRows.getList().subList(0, orderedRows.getCount() - 1);
            }
            for (CounterRow<ByteBuffer, ByteBuffer> row : rows) {
                clearCounterRow(ksp, counterColumnFamilyName, row.getKey());
            }
        }
    }

    private static void clearCounterRow(Keyspace ksp, String counterColumnFamilyName, ByteBuffer key) {
        ByteBufferSerializer bs = ByteBufferSerializer.get();
        StringSerializer ss = StringSerializer.get();

        SliceCounterQuery<ByteBuffer, ByteBuffer> query = HFactory.createCounterSliceQuery(ksp, bs, bs);
        boolean finished = false;
        ByteBuffer lastColumnName = null;
        Mutator<ByteBuffer> mutator = HFactory.createMutator(ksp, ByteBufferSerializer.get());
        while (!finished) {
            query.setColumnFamily(counterColumnFamilyName);
            query.setKey(key);
            query.setRange(lastColumnName, null, false, 50);
            CounterSlice<ByteBuffer> slice = query.execute().get();
            List<HCounterColumn<ByteBuffer>> columns = slice.getColumns();
            if (columns.size() < 50) finished = true;
            for (HCounterColumn<ByteBuffer> column : columns) {
                mutator.addCounter(key, counterColumnFamilyName, HFactory.createCounterColumn(column.getName(), -column.getValue(), bs));
            }
        }
        mutator.execute();
    }
}

