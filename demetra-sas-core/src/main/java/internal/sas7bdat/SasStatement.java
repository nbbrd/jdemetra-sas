/*
 * Copyright 2013 National Bank of Belgium
 *
 * Licensed under the EUPL, Version 1.1 or - as soon they will be approved
 * by the European Commission - subsequent versions of the EUPL (the "Licence");
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at:
 *
 * http://ec.europa.eu/idabc/eupl
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the Licence is distributed on an "AS IS" basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Licence for the specific language governing permissions and
 * limitations under the Licence.
 */
package internal.sas7bdat;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;
import javax.annotation.Nonnull;
import java.util.SortedMap;
import java.util.SortedSet;

import ec.tss.tsproviders.utils.IteratorWithIO;
import internal.xdb.DbBasicSelect;
import internal.xdb.DbRawDataUtil;
import internal.xdb.DbRawDataUtil.SuperDataType;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;

import sasquatch.SasColumn;
import sasquatch.SasForwardCursor;
import sasquatch.SasMetaData;
import sasquatch.Sasquatch;

/**
 * @author Philippe Charles
 */
@lombok.AllArgsConstructor
public final class SasStatement implements Closeable {

    @lombok.NonNull
    private final Sasquatch sasquatch;

    @lombok.NonNull
    private final Path conn;

    @Nonnull
    public SasForwardCursor executeQuery(@Nonnull DbBasicSelect query) throws IOException {
        SasForwardCursor input = sasquatch.readForward(conn.resolve(query.getTableName()));

        Function<String, SasColumn> toColumn = toColumnByName(input.getColumns());
        ToIntFunction<SasColumn> toInternalIndex = SasColumn::getOrder;
        Function<SasColumn, SuperDataType> toDataType = o -> SuperDataType.COMPARABLE;

        List<SasColumn> selectColumns = DbRawDataUtil.getColumns(toColumn, query.getSelectColumns());
        List<SasColumn> orderColumns = DbRawDataUtil.getColumns(toColumn, query.getOrderColumns());
        SortedMap<SasColumn, String> filter = DbRawDataUtil.getFilter(toInternalIndex, toColumn, query.getFilterItems());

        SortedSet<SasColumn> dataColumns = DbRawDataUtil.mergeAndSort(toInternalIndex, selectColumns, orderColumns);
        ToIntFunction<SasColumn> toIndex = DbRawDataUtil.ToIndex.of(toInternalIndex, dataColumns);

        IteratorWithIO<Object[]> rows = getRows(input, dataColumns, filter);

        if (query.isDistinct()) {
            BiConsumer<Object[], Object[]> aggregator = DbRawDataUtil.NO_AGGREGATION;
            rows = DbRawDataUtil.distinct(rows, selectColumns, toIndex, toDataType, aggregator);
        }

        if (DbRawDataUtil.isSortRequired(query.isDistinct(), selectColumns, orderColumns)) {
            rows = DbRawDataUtil.sort(rows, orderColumns, toIndex, toDataType);
        }

        return new CustomResultSet(rows, selectColumns, DbRawDataUtil.createIndexes(selectColumns, toIndex), input);
    }

    @Override
    public void close() throws IOException {
    }

    //<editor-fold defaultstate="collapsed" desc="Internal implementation">
    private static IteratorWithIO<Object[]> getRows(SasForwardCursor input, SortedSet<SasColumn> dataColumns, SortedMap<SasColumn, String> filter) throws IOException {
        return filter.isEmpty()
                ? new BasicIterator(input, dataColumns)
                : new FilteredIterator(input, dataColumns, filter);
    }

    private static Function<String, SasColumn> toColumnByName(List<SasColumn> columns) {
        Map<String, SasColumn> result = columns
                .stream()
                .collect(Collectors.toMap(o -> o.getName().toLowerCase(Locale.ROOT), o -> o));
        return o -> result.get(o.toLowerCase(Locale.ROOT));
    }

    private static final class CustomResultSet implements SasForwardCursor {

        private final int[] indexes;
        private final IteratorWithIO<Object[]> rows;
        private final Closeable closeable;
        private Object[] cursor;
        private final SasMetaData metaData;

        public CustomResultSet(IteratorWithIO<Object[]> rows, List<SasColumn> select, int[] indexes, Closeable closeable) {
            SasMetaData.Builder mb = SasMetaData.builder();
            for (int i = 0; i < select.size(); i++) {
                mb.column(select.get(i).toBuilder().order(i).build());
            }
            this.metaData = mb.build();
            this.indexes = indexes;
            this.rows = rows;
            this.closeable = closeable;
            this.cursor = null;
        }

        @Override
        public SasMetaData getMetaData() throws IOException {
            return metaData;
        }

        @Override
        public boolean next() throws IOException {
            if (rows.hasNext()) {
                cursor = rows.next();
                return true;
            }
            return false;
        }

        @Override
        public Object getValue(int columnIndex) throws IOException, IndexOutOfBoundsException {
            return cursor[indexes[columnIndex]];
        }

        @Override
        public double getNumber(int columnIndex) throws IOException, IndexOutOfBoundsException, IllegalArgumentException {
            Number value = (Number) getValue(columnIndex);
            return value != null ? value.doubleValue() : Double.NaN;
        }

        @Override
        public String getString(int columnIndex) throws IOException, IndexOutOfBoundsException, IllegalArgumentException {
            return (String) getValue(columnIndex);
        }

        @Override
        public LocalDate getDate(int columnIndex) throws IOException, IndexOutOfBoundsException, IllegalArgumentException {
            return (LocalDate) getValue(columnIndex);
        }

        @Override
        public LocalDateTime getDateTime(int columnIndex) throws IOException, IndexOutOfBoundsException, IllegalArgumentException {
            return (LocalDateTime) getValue(columnIndex);
        }

        @Override
        public LocalTime getTime(int columnIndex) throws IOException, IndexOutOfBoundsException, IllegalArgumentException {
            return (LocalTime) getValue(columnIndex);
        }

        @Override
        public Object[] getValues() throws IOException {
            return cursor.clone();
        }

        @Override
        public void close() throws IOException {
            closeable.close();
        }
    }

    private static class BasicIterator extends AbstractIteratorWithIO<Object[]> {

        protected final SasForwardCursor rs;
        protected final SasColumn[] dataColumns;

        public BasicIterator(SasForwardCursor rs, SortedSet<SasColumn> dataColumns) {
            this.rs = rs;
            this.dataColumns = dataColumns.toArray(new SasColumn[0]);
        }

        @Override
        protected Object[] computeNext() throws IOException {
            if (rs.next()) {
                Object[] row = new Object[dataColumns.length];
                for (int i = 0; i < row.length; i++) {
                    row[i] = rs.getValue(dataColumns[i].getOrder());
                }
                return row;
            }
            return endOfData();
        }

        @Override
        public void close() throws IOException {
            // SasResultSet closed somewhere else
        }
    }

    private static final class FilteredIterator extends BasicIterator {

        protected final SortedMap<SasColumn, String> filter;
        protected final Object[] cache;

        public FilteredIterator(SasForwardCursor rs, SortedSet<SasColumn> dataColumns, SortedMap<SasColumn, String> filter) throws IOException {
            super(rs, dataColumns);
            this.filter = filter;
            this.cache = new Object[rs.getColumns().size()];
        }

        Object getCachedValue(int columnIndex) throws IOException {
            Object result = cache[columnIndex];
            return result != null ? result : rs.getValue(columnIndex);
        }

        @Override
        protected Object[] computeNext() throws IOException {
            while (rs.next()) {
                if (isValidRow()) {
                    Object[] result = new Object[dataColumns.length];
                    for (int i = 0; i < result.length; i++) {
                        result[i] = getCachedValue(dataColumns[i].getOrder());
                    }
                    return result;
                }
            }
            return endOfData();
        }

        boolean isValidRow() throws IOException {
            for (Entry<SasColumn, String> o : filter.entrySet()) {
                int index = o.getKey().getOrder();
                cache[index] = rs.getValue(index);
                if (!Objects.equals(cache[index], o.getValue())) {
                    return false;
                }
            }
            return true;
        }
    }
    //</editor-fold>
}
