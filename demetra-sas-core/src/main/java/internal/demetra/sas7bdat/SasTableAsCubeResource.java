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

import internal.sas7bdat.SasStatement;
import ec.tss.tsproviders.HasFilePaths;
import ec.tss.tsproviders.cube.CubeId;
import ec.tss.tsproviders.cube.TableAsCubeAccessor;
import ec.tss.tsproviders.cube.TableAsCubeUtil;
import ec.tss.tsproviders.cube.TableDataParams;
import ec.tss.tsproviders.utils.ObsCharacteristics;
import ec.tss.tsproviders.utils.ObsGathering;
import ec.tss.tsproviders.utils.OptionalTsData;
import ec.tstoolkit.design.VisibleForTesting;
import static internal.demetra.sas7bdat.SasFunc.onDate;
import static internal.demetra.sas7bdat.SasFunc.onGetObjectToString;
import static internal.demetra.sas7bdat.SasFunc.onGetStringArray;
import static internal.demetra.sas7bdat.SasFunc.onNull;
import static internal.demetra.sas7bdat.SasFunc.onNumber;
import internal.xdb.DbBasicSelect;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AccessLevel;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import sasquatch.SasForwardCursor;
import sasquatch.Sasquatch;

/**
 *
 * @author Philippe Charles
 */
@lombok.AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class SasTableAsCubeResource implements TableAsCubeAccessor.Resource<LocalDate> {

    @NonNull
    public static SasTableAsCubeResource create(
            @NonNull Sasquatch sasquatch,
            @NonNull HasFilePaths paths,
            @NonNull File folder,
            @NonNull String table,
            @NonNull List<String> dimColumns,
            @NonNull TableDataParams tdp,
            @NonNull ObsGathering gathering,
            @NonNull String labelColumn) {
        return new SasTableAsCubeResource(sasquatch, paths, folder, table, CubeId.root(dimColumns), tdp, gathering, labelColumn);
    }

    private final Sasquatch sasquatch;
    private final HasFilePaths paths;
    private final File folder;
    private final String fileName;
    private final CubeId root;
    private final TableDataParams tdp;
    private final ObsGathering gathering;
    private final String labelColumn;

    @Override
    public Exception testConnection() {
        return null;
    }

    @Override
    public CubeId getRoot() {
        return root;
    }

    @Override
    public TableAsCubeAccessor.AllSeriesCursor getAllSeriesCursor(CubeId id) throws Exception {
        return new AllSeriesQuery(id, fileName, labelColumn).call(sasquatch, paths, folder);
    }

    @Override
    public TableAsCubeAccessor.AllSeriesWithDataCursor<LocalDate> getAllSeriesWithDataCursor(CubeId id) throws Exception {
        return new AllSeriesWithDataQuery(id, fileName, labelColumn, tdp).call(sasquatch, paths, folder);
    }

    @Override
    public TableAsCubeAccessor.SeriesWithDataCursor<LocalDate> getSeriesWithDataCursor(CubeId id) throws Exception {
        return new SeriesWithDataQuery(id, fileName, labelColumn, tdp).call(sasquatch, paths, folder);
    }

    @Override
    public TableAsCubeAccessor.ChildrenCursor getChildrenCursor(CubeId id) throws Exception {
        return new ChildrenQuery(id, fileName).call(sasquatch, paths, folder);
    }

    @Override
    public String getDisplayName() throws Exception {
        return TableAsCubeUtil.getDisplayName(folder.getPath(), fileName, tdp.getValueColumn(), gathering);
    }

    @Override
    public String getDisplayName(CubeId id) throws Exception {
        return TableAsCubeUtil.getDisplayName(id, SasTableAsCubeUtil.LABEL_COLLECTOR);
    }

    @Override
    public String getDisplayNodeName(CubeId id) throws Exception {
        return TableAsCubeUtil.getDisplayNodeName(id);
    }

    @Override
    public OptionalTsData.Builder2<LocalDate> newBuilder() {
        return OptionalTsData.builderByLocalDate(gathering, ObsCharacteristics.ORDERED);
    }

    private static void closeAll(Exception root, AutoCloseable... items) {
        for (AutoCloseable o : items) {
            if (o != null) {
                try {
                    o.close();
                } catch (Exception ex) {
                    if (root == null) {
                        root = ex;
                    } else {
                        root.addSuppressed(ex);
                    }
                }
            }
        }
    }

    private static AutoCloseable asCloseable(SasForwardCursor rs, SasStatement stmt) {
        return () -> closeAll(null, rs, stmt);
    }

    private static String[] toSelect(CubeId ref) {
        String[] result = new String[ref.getDepth()];
        for (int i = 0; i < result.length; i++) {
            result[i] = ref.getDimensionId(ref.getLevel() + i);
        }
        return result;
    }

    private static Map<String, String> toFilter(CubeId id) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < id.getLevel(); i++) {
            result.put(id.getDimensionId(i), id.getDimensionValue(i));
        }
        return result;
    }

    @VisibleForTesting
    interface SasQuery<T> {

        @NonNull
        DbBasicSelect getQuery();

        @Nullable
        T process(@NonNull SasForwardCursor rs, @NonNull AutoCloseable closeable) throws IOException;

        @Nullable
        default T call(@NonNull Sasquatch sasquatch, @NonNull HasFilePaths paths, @NonNull File folder) throws IOException {
            SasStatement stmt = null;
            SasForwardCursor rs = null;
            try {
                Path conn = paths.resolveFilePath(folder).toPath();
                stmt = new SasStatement(sasquatch, conn);
                DbBasicSelect query = getQuery();
                rs = stmt.executeQuery(query);
                return process(rs, asCloseable(rs, stmt));
            } catch (IOException ex) {
                closeAll(ex, rs, stmt);
                throw ex;
            }
        }
    }

    private static final class AllSeriesQuery implements SasQuery<TableAsCubeAccessor.AllSeriesCursor> {

        private final CubeId ref;
        private final String table;
        private final String label;

        AllSeriesQuery(CubeId id, String table, String label) {
            this.ref = id;
            this.table = table;
            this.label = label;
        }

        @Override
        public DbBasicSelect getQuery() {
            return DbBasicSelect.from(table)
                    .distinct(true)
                    .select(toSelect(ref)).select(label)
                    .filter(toFilter(ref))
                    .orderBy(toSelect(ref))
                    .build();
        }

        @Override
        public TableAsCubeAccessor.AllSeriesCursor process(SasForwardCursor rs, AutoCloseable closeable) throws IOException {
            SasFunc<String[]> toDimValues = onGetStringArray(0, ref.getDepth());
            SasFunc<String> toLabel = !label.isEmpty() ? onGetObjectToString(1) : onNull();

            return SasTableAsCubeUtil.allSeriesCursor(rs, closeable, toDimValues, toLabel, ref);
        }
    }

    private static final class AllSeriesWithDataQuery implements SasQuery<TableAsCubeAccessor.AllSeriesWithDataCursor<LocalDate>> {

        private final CubeId ref;
        private final String table;
        private final String label;
        private final TableDataParams tdp;

        AllSeriesWithDataQuery(CubeId id, String table, String label, TableDataParams tdp) {
            this.ref = id;
            this.table = table;
            this.label = label;
            this.tdp = tdp;
        }

        @Override
        public DbBasicSelect getQuery() {
            return DbBasicSelect.from(table)
                    .select(toSelect(ref)).select(tdp.getPeriodColumn(), tdp.getValueColumn()).select(label)
                    .filter(toFilter(ref))
                    .orderBy(toSelect(ref)).orderBy(tdp.getPeriodColumn(), tdp.getVersionColumn())
                    .build();
        }

        @Override
        public TableAsCubeAccessor.AllSeriesWithDataCursor<LocalDate> process(SasForwardCursor rs, AutoCloseable closeable) throws IOException {
            SasFunc<String[]> toDimValues = onGetStringArray(0, ref.getDepth());
            SasFunc<LocalDate> toPeriod = onDate(rs, ref.getDepth(), Parsers2.dateTimeParser(tdp.getObsFormat(), LocalDate::from));
            SasFunc<Number> toValue = onNumber(rs, ref.getDepth() + 1, tdp.getObsFormat().numberParser());
            SasFunc<String> toLabel = !label.isEmpty() ? onGetObjectToString(ref.getDepth() + 2) : onNull();

            return SasTableAsCubeUtil.allSeriesWithDataCursor(rs, closeable, toDimValues, toPeriod, toValue, toLabel, ref);
        }
    }

    private static final class SeriesWithDataQuery implements SasQuery<TableAsCubeAccessor.SeriesWithDataCursor<LocalDate>> {

        private final CubeId ref;
        private final String table;
        private final String label;
        private final TableDataParams tdp;

        SeriesWithDataQuery(CubeId id, String table, String label, TableDataParams tdp) {
            this.ref = id;
            this.table = table;
            this.label = label;
            this.tdp = tdp;
        }

        @Override
        public DbBasicSelect getQuery() {
            return DbBasicSelect.from(table)
                    .select(tdp.getPeriodColumn(), tdp.getValueColumn()).select(label)
                    .filter(toFilter(ref))
                    .orderBy(tdp.getPeriodColumn(), tdp.getVersionColumn())
                    .build();
        }

        @Override
        public TableAsCubeAccessor.SeriesWithDataCursor<LocalDate> process(SasForwardCursor rs, AutoCloseable closeable) throws IOException {
            SasFunc<LocalDate> toPeriod = SasFunc.onDate(rs, 0, Parsers2.dateTimeParser(tdp.getObsFormat(), LocalDate::from));
            SasFunc<Number> toValue = onNumber(rs, 1, tdp.getObsFormat().numberParser());
            SasFunc<String> toLabel = !label.isEmpty() ? onGetObjectToString(2) : onNull();

            return SasTableAsCubeUtil.seriesWithDataCursor(rs, closeable, toPeriod, toValue, toLabel, ref);
        }
    }

    private static final class ChildrenQuery implements SasQuery<TableAsCubeAccessor.ChildrenCursor> {

        private final CubeId ref;
        private final String table;

        ChildrenQuery(CubeId id, String table) {
            this.ref = id;
            this.table = table;
        }

        @Override
        public DbBasicSelect getQuery() {
            String column = ref.getDimensionId(ref.getLevel());
            return DbBasicSelect.from(table)
                    .distinct(true)
                    .select(column)
                    .filter(toFilter(ref))
                    .orderBy(column)
                    .build();
        }

        @Override
        public TableAsCubeAccessor.ChildrenCursor process(SasForwardCursor rs, AutoCloseable closeable) throws IOException {
            SasFunc<String> toChild = onGetObjectToString(0);

            return SasTableAsCubeUtil.childrenCursor(rs, closeable, toChild, ref);
        }
    }
}
