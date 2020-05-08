/*
 * Copyright 2017 National Bank of Belgium
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
package internal.demetra.sas7bdat;

import ec.tss.tsproviders.cube.CubeId;
import ec.tss.tsproviders.cube.TableAsCubeAccessor.AllSeriesCursor;
import ec.tss.tsproviders.cube.TableAsCubeAccessor.AllSeriesWithDataCursor;
import ec.tss.tsproviders.cube.TableAsCubeAccessor.ChildrenCursor;
import ec.tss.tsproviders.cube.TableAsCubeAccessor.SeriesCursor;
import ec.tss.tsproviders.cube.TableAsCubeAccessor.SeriesWithDataCursor;
import ec.tss.tsproviders.cube.TableAsCubeAccessor.TableCursor;
import java.io.IOException;
import java.time.LocalDate;
import java.util.Collections;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import sasquatch.SasForwardCursor;

/**
 *
 * @author Philippe Charles
 */
@lombok.experimental.UtilityClass
class SasTableAsCubeUtil {

    final Collector<? super String, ?, String> LABEL_COLLECTOR = Collectors.joining(", ");

    AllSeriesCursor allSeriesCursor(SasForwardCursor rs, AutoCloseable closeable, SasFunc<String[]> toDimValues, SasFunc<String> toLabel, CubeId ref) {
        return new ResultSetAllSeriesCursor(rs, closeable, toDimValues, toLabel, ref);
    }

    AllSeriesWithDataCursor<LocalDate> allSeriesWithDataCursor(SasForwardCursor rs, AutoCloseable closeable, SasFunc<String[]> toDimValues, SasFunc<LocalDate> toPeriod, SasFunc<Number> toValue, SasFunc<String> toLabel, CubeId ref) {
        return new ResultSetAllSeriesWithDataCursor(rs, closeable, toDimValues, toPeriod, toValue, toLabel, ref);
    }

    SeriesWithDataCursor<LocalDate> seriesWithDataCursor(SasForwardCursor rs, AutoCloseable closeable, SasFunc<LocalDate> toPeriod, SasFunc<Number> toValue, SasFunc<String> toLabel, CubeId ref) {
        return new ResultSetSeriesWithDataCursor(rs, closeable, toPeriod, toValue, toLabel, ref);
    }

    ChildrenCursor childrenCursor(SasForwardCursor rs, AutoCloseable closeable, SasFunc<String> toChild, CubeId ref) {
        return new ResultSetChildrenCursor(rs, closeable, toChild, ref);
    }

    //<editor-fold defaultstate="collapsed" desc="Implementation details">
    private static abstract class ResultSetTableCursor implements TableCursor {

        private final SasForwardCursor rs;
        private final AutoCloseable closeable;
        private boolean closed;

        private ResultSetTableCursor(SasForwardCursor rs, AutoCloseable closeable) {
            this.rs = rs;
            this.closeable = closeable;
            this.closed = false;
        }

        protected abstract void processRow(SasForwardCursor rs) throws IOException;

        @Override
        public boolean isClosed() throws Exception {
            return closed;
        }

        @Override
        public boolean nextRow() throws Exception {
            boolean result = rs.next();
            if (result) {
                processRow(rs);
            }
            return result;
        }

        @Override
        public void close() throws Exception {
            closed = true;
            closeable.close();
        }
    }

    private static abstract class ResultSetSeriesCursor extends ResultSetTableCursor implements SeriesCursor {

        private ResultSetSeriesCursor(SasForwardCursor rs, AutoCloseable closeable) {
            super(rs, closeable);
        }

        @Override
        public Map<String, String> getMetaData() throws Exception {
            return Collections.emptyMap();
        }
    }

    private static final class ResultSetAllSeriesCursor extends ResultSetSeriesCursor implements AllSeriesCursor {

        private final SasFunc<String[]> toDimValues;
        private final SasFunc<String> toLabel;
        private final CubeId ref;
        private String[] dimValues;
        private String label;

        private ResultSetAllSeriesCursor(SasForwardCursor rs, AutoCloseable closeable, SasFunc<String[]> toDimValues, SasFunc<String> toLabel, CubeId ref) {
            super(rs, closeable);
            this.toDimValues = toDimValues;
            this.toLabel = toLabel;
            this.ref = ref;
            this.dimValues = null;
            this.label = null;
        }

        @Override
        public String getLabel() throws Exception {
            return label != null ? label : Stream.concat(ref.getDimensionValueStream(), Stream.of(dimValues)).collect(LABEL_COLLECTOR);
        }

        @Override
        public String[] getDimValues() throws Exception {
            return dimValues;
        }

        @Override
        protected void processRow(SasForwardCursor rs) throws IOException {
            dimValues = toDimValues.apply(rs);
            label = toLabel.apply(rs);
        }
    }

    private static final class ResultSetAllSeriesWithDataCursor extends ResultSetSeriesCursor implements AllSeriesWithDataCursor<LocalDate> {

        private final SasFunc<String[]> toDimValues;
        private final SasFunc<LocalDate> toPeriod;
        private final SasFunc<Number> toValue;
        private final SasFunc<String> toLabel;
        private final CubeId ref;
        private String[] dimValues;
        private LocalDate period;
        private Number value;
        private String label;

        private ResultSetAllSeriesWithDataCursor(SasForwardCursor rs, AutoCloseable closeable, SasFunc<String[]> toDimValues, SasFunc<LocalDate> toPeriod, SasFunc<Number> toValue, SasFunc<String> toLabel, CubeId ref) {
            super(rs, closeable);
            this.toDimValues = toDimValues;
            this.toPeriod = toPeriod;
            this.toValue = toValue;
            this.toLabel = toLabel;
            this.ref = ref;
            this.dimValues = null;
            this.period = null;
            this.value = null;
            this.label = null;
        }

        @Override
        public String getLabel() throws Exception {
            return label != null ? label : Stream.concat(ref.getDimensionValueStream(), Stream.of(dimValues)).collect(LABEL_COLLECTOR);
        }

        @Override
        public String[] getDimValues() throws Exception {
            return dimValues;
        }

        @Override
        public LocalDate getPeriod() throws Exception {
            return period;
        }

        @Override
        public Number getValue() throws Exception {
            return value;
        }

        @Override
        protected void processRow(SasForwardCursor rs) throws IOException {
            dimValues = toDimValues.apply(rs);
            period = toPeriod.apply(rs);
            value = period != null ? toValue.apply(rs) : null;
            label = toLabel.apply(rs);
        }
    }

    private static final class ResultSetSeriesWithDataCursor extends ResultSetSeriesCursor implements SeriesWithDataCursor<LocalDate> {

        private final SasFunc<LocalDate> toPeriod;
        private final SasFunc<Number> toValue;
        private final SasFunc<String> toLabel;
        private final CubeId ref;
        private LocalDate period;
        private Number value;
        private String label;

        private ResultSetSeriesWithDataCursor(SasForwardCursor rs, AutoCloseable closeable, SasFunc<LocalDate> toPeriod, SasFunc<Number> toValue, SasFunc<String> toLabel, CubeId ref) {
            super(rs, closeable);
            this.toPeriod = toPeriod;
            this.toValue = toValue;
            this.toLabel = toLabel;
            this.ref = ref;
            this.period = null;
            this.value = null;
            this.label = null;
        }

        @Override
        public String getLabel() throws Exception {
            return label != null ? label : ref.getDimensionValueStream().collect(LABEL_COLLECTOR);
        }

        @Override
        public LocalDate getPeriod() throws Exception {
            return period;
        }

        @Override
        public Number getValue() throws Exception {
            return value;
        }

        @Override
        protected void processRow(SasForwardCursor rs) throws IOException {
            period = toPeriod.apply(rs);
            value = period != null ? toValue.apply(rs) : null;
            label = toLabel.apply(rs);
        }
    }

    private static final class ResultSetChildrenCursor extends ResultSetTableCursor implements ChildrenCursor {

        private final SasFunc<String> toChild;
        private final CubeId ref;
        private String child;

        private ResultSetChildrenCursor(SasForwardCursor rs, AutoCloseable closeable, SasFunc<String> toChild, CubeId ref) {
            super(rs, closeable);
            this.toChild = toChild;
            this.ref = ref;
            this.child = null;
        }

        @Override
        public String getChild() throws Exception {
            return child;
        }

        @Override
        protected void processRow(SasForwardCursor rs) throws IOException {
            child = toChild.apply(rs);
        }
    }
    //</editor-fold>
}
