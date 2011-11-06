package me.prettyprint.hector.api.deletion;

import me.prettyprint.cassandra.serializers.ByteBufferSerializer;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.*;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.RangeSuperSlicesCounterQuery;
import me.prettyprint.hector.api.query.RangeSuperSlicesQuery;
import me.prettyprint.hector.api.query.SuperSliceCounterQuery;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


public class SuperColumnFamilyDelete {

    public static void clearSuperColumnFamily(Keyspace ksp, String superColumnFamilyName) {
        ByteBufferSerializer bs = ByteBufferSerializer.get();

        boolean finished = false;
        ByteBuffer lastKey = null;

        RangeSuperSlicesQuery<ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer> query = HFactory.createRangeSuperSlicesQuery(ksp, bs, bs, bs, bs);
        while (!finished) {
            query.setColumnFamily(superColumnFamilyName);
            query.setKeys(lastKey, null);
            query.setRowCount(51);
            query.setRange(null, null, false, 1);
            OrderedSuperRows<ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer> orderedSuperRows = query.execute().get();
            List<SuperRow<ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer>> rows;
            if (orderedSuperRows.getCount() < 51) {
                finished = true;
                rows = orderedSuperRows.getList();
            } else {
                List<SuperRow<ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer>> temp = orderedSuperRows.getList();
                lastKey = temp.get(temp.size() - 1).getKey();
                rows = temp.subList(0, temp.size() - 2);
            }

            for (SuperRow<ByteBuffer, ByteBuffer, ByteBuffer, ByteBuffer> row : rows) {
                Mutator<ByteBuffer> mutator = HFactory.createMutator(ksp, ByteBufferSerializer.get());
                mutator.delete(row.getKey(), superColumnFamilyName, null, bs);
                mutator.execute();
            }
        }
    }

    public static void clearSuperCounterColumnFamily(Keyspace ksp, String superCounterColumnFamilyName) {
        ByteBufferSerializer bs = ByteBufferSerializer.get();

        boolean finished = false;
        ByteBuffer lastKey = null;

        RangeSuperSlicesCounterQuery<ByteBuffer, ByteBuffer, ByteBuffer> query = HFactory.createRangeSuperSlicesCounterQuery(ksp, bs, bs, bs);
        while (!finished) {
            query.setColumnFamily(superCounterColumnFamilyName);
            query.setKeys(lastKey, null);
            query.setRowCount(51);
            query.setRange(null, null, false, 1);
            OrderedCounterSuperRows<ByteBuffer, ByteBuffer, ByteBuffer> superRows = query.execute().get();
            List<CounterSuperRow<ByteBuffer, ByteBuffer, ByteBuffer>> rows;
            if (superRows.getCount() < 51) {
                rows = superRows.getList();
                finished = true;

            } else {
                List<CounterSuperRow<ByteBuffer, ByteBuffer, ByteBuffer>> fullList = superRows.getList();
                lastKey = fullList.get(fullList.size() - 1).getKey();
                rows = fullList.subList(0, fullList.size() - 2);
            }

            for (CounterSuperRow<ByteBuffer, ByteBuffer, ByteBuffer> row : rows) {
                clearSupercounterColumnRow(ksp, superCounterColumnFamilyName, row.getKey());
            }
        }
    }

    public static void clearSupercounterColumnRow(Keyspace ksp, String superCounterColumnFamilyName, ByteBuffer key) {
        ByteBufferSerializer bs = ByteBufferSerializer.get();

        ByteBuffer lastColumn = null;
        boolean finished = false;
        SuperSliceCounterQuery<ByteBuffer, ByteBuffer, ByteBuffer> query = HFactory.createSuperSliceCounterQuery(ksp, bs, bs, bs);
        query.setColumnFamily(superCounterColumnFamilyName);
        query.setKey(key);

        while (!finished) {
            query.setRange(lastColumn, null, false, 100);
            CounterSuperSlice<ByteBuffer, ByteBuffer> queryResult = query.execute().get();
            List<HCounterSuperColumn<ByteBuffer, ByteBuffer>> superColumns = queryResult.getSuperColumns();
            // the error maybe here
            if (superColumns.size() > 0) lastColumn = superColumns.get(superColumns.size() - 1).getName();
            if (superColumns.size() < 100) finished = true;

            for (HCounterSuperColumn<ByteBuffer, ByteBuffer> superColumn : superColumns) {
                List<HCounterColumn<ByteBuffer>> columns = superColumn.getColumns();

                List<HCounterColumn<ByteBuffer>> inverseColumns = new ArrayList<HCounterColumn<ByteBuffer>>();

                for (HCounterColumn<ByteBuffer> column : columns) {
                    if (column.getValue().equals(0L)) continue;
                    Long value = -column.getValue();
                    ByteBuffer name = column.getName();
                    inverseColumns.add(HFactory.createCounterColumn(name, value, bs));
                }
                Mutator<ByteBuffer> eraser = HFactory.createMutator(ksp, ByteBufferSerializer.get());
                HCounterSuperColumn<ByteBuffer, ByteBuffer> cleanSuperColumn =
                        HFactory.createCounterSuperColumn(superColumn.getName(), inverseColumns, bs, bs);
                eraser.insertCounter(key, superCounterColumnFamilyName, cleanSuperColumn);
                eraser.execute();
            }
        }
    }
}
