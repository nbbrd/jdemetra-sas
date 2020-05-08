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
package be.nbb.demetra.sas;

import internal.demetra.sas7bdat.SasFunc;
import internal.sas7bdat.SasStatement;
import com.google.common.cache.Cache;
import ec.tss.tsproviders.db.DbAccessor;
import ec.tss.tsproviders.db.DbSeries;
import ec.tss.tsproviders.db.DbSetId;
import ec.tss.tsproviders.db.DbUtil;
import ec.tstoolkit.utilities.LastModifiedFileCache;
import internal.xdb.DbBasicSelect;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import sasquatch.SasForwardCursor;
import sasquatch.Sasquatch;

/**
 *
 * @author Philippe Charles
 */
@Deprecated
final class SasAccessor extends DbAccessor.Commander<SasBean> {

    private final Sasquatch sasquatch;

    public SasAccessor(Sasquatch sasquatch, SasBean dbBean) {
        super(dbBean);
        this.sasquatch = sasquatch;
    }

    @Override
    protected Callable<List<DbSetId>> getAllSeriesQuery(DbSetId ref) {
        return new SasQuery<List<DbSetId>>(ref) {
            @Override
            protected DbBasicSelect getQuery() {
                return DbBasicSelect
                        .from(dbBean.getTableName())
                        .distinct(true)
                        .select(ref.selectColumns())
                        .filter(toFilter(ref))
                        .orderBy(ref.selectColumns())
                        .build();
            }

            @Override
            protected List<DbSetId> process(final SasForwardCursor rs) throws IOException {
                SasFunc<String[]> toDimValues = SasFunc.onGetStringArray(0, ref.getDepth());

                DbUtil.AllSeriesCursor<IOException> cursor = new DbUtil.AllSeriesCursor<IOException>() {
                    @Override
                    public boolean next() throws IOException {
                        boolean result = rs.next();
                        if (result) {
                            dimValues = toDimValues.apply(rs);
                        }
                        return result;
                    }
                };

                return DbUtil.getAllSeries(cursor, ref);
            }
        };
    }

    @Override
    protected Callable<List<DbSeries>> getAllSeriesWithDataQuery(DbSetId ref) {
        return new SasQuery<List<DbSeries>>(ref) {
            @Override
            protected DbBasicSelect getQuery() {
                DbBasicSelect.Builder result = DbBasicSelect
                        .from(dbBean.getTableName())
                        .select(ref.selectColumns()).select(dbBean.getPeriodColumn(), dbBean.getValueColumn())
                        .filter(toFilter(ref))
                        .orderBy(ref.selectColumns());
                if (!dbBean.getVersionColumn().isEmpty()) {
                    result.orderBy(dbBean.getPeriodColumn(), dbBean.getVersionColumn());
                }
                return result.build();
            }

            @Override
            protected List<DbSeries> process(final SasForwardCursor rs) throws IOException {
                SasFunc<String[]> toDimValues = SasFunc.onGetStringArray(0, ref.getDepth());
                SasFunc<java.util.Date> toPeriod = SasFunc.onCalendar(rs, ref.getDepth(), dateParser);
                SasFunc<Number> toValue = SasFunc.onNumber(rs, ref.getDepth() + 1, numberParser);

                DbUtil.AllSeriesWithDataCursor<IOException> cursor = new DbUtil.AllSeriesWithDataCursor<IOException>() {
                    @Override
                    public boolean next() throws IOException {
                        boolean result = rs.next();
                        if (result) {
                            dimValues = toDimValues.apply(rs);
                            period = toPeriod.apply(rs);
                            value = period != null ? toValue.apply(rs) : null;
                        }
                        return result;
                    }
                };

                return DbUtil.getAllSeriesWithData(cursor, ref, dbBean.getFrequency(), dbBean.getAggregationType());
            }
        };
    }

    @Override
    protected Callable<DbSeries> getSeriesWithDataQuery(DbSetId ref) {
        return new SasQuery<DbSeries>(ref) {
            @Override
            protected DbBasicSelect getQuery() {
                DbBasicSelect.Builder result = DbBasicSelect.from(dbBean.getTableName())
                        .select(dbBean.getPeriodColumn(), dbBean.getValueColumn())
                        .filter(toFilter(ref));
                if (!dbBean.getVersionColumn().isEmpty()) {
                    result.orderBy(dbBean.getPeriodColumn(), dbBean.getVersionColumn());
                }
                return result.build();
            }

            @Override
            protected DbSeries process(final SasForwardCursor rs) throws IOException {
                SasFunc<Date> toPeriod = SasFunc.onCalendar(rs, 0, dateParser);
                SasFunc<Number> toValue = SasFunc.onNumber(rs, 1, numberParser);

                DbUtil.SeriesWithDataCursor<IOException> cursor = new DbUtil.SeriesWithDataCursor<IOException>() {
                    @Override
                    public boolean next() throws IOException {
                        boolean result = rs.next();
                        if (result) {
                            period = toPeriod.apply(rs);
                            value = period != null ? toValue.apply(rs) : null;
                            return true;
                        }
                        return false;
                    }
                };

                return DbUtil.getSeriesWithData(cursor, ref, dbBean.getFrequency(), dbBean.getAggregationType());
            }
        };
    }

    @Override
    protected Callable<List<String>> getChildrenQuery(DbSetId ref) {
        return new SasQuery<List<String>>(ref) {
            @Override
            protected DbBasicSelect getQuery() {
                String column = ref.getColumn(ref.getLevel());
                return DbBasicSelect
                        .from(dbBean.getTableName())
                        .distinct(true)
                        .select(column)
                        .filter(toFilter(ref))
                        .orderBy(column)
                        .build();
            }

            @Override
            protected List<String> process(final SasForwardCursor rs) throws IOException {
                SasFunc<String> toChild = SasFunc.onGetObjectToString(0);

                DbUtil.ChildrenCursor<IOException> cursor = new DbUtil.ChildrenCursor<IOException>() {
                    @Override
                    public boolean next() throws IOException {
                        boolean result = rs.next();
                        if (result) {
                            child = toChild.apply(rs);
                            return true;
                        }
                        return false;
                    }
                };

                return DbUtil.getChildren(cursor);
            }
        };
    }

    @Override
    public DbAccessor<SasBean> memoize() {
        File dataset = dbBean.getTableFile();
        Cache<DbSetId, List<DbSeries>> ttl = DbAccessor.BulkAccessor.newTtlCache(dbBean.getCacheTtl());
        Cache<DbSetId, List<DbSeries>> file = LastModifiedFileCache.from(dataset, ttl);
        return DbAccessor.BulkAccessor.from(this, dbBean.getCacheDepth(), file);
    }

    private abstract class SasQuery<T> implements Callable<T> {

        protected final DbSetId ref;

        public SasQuery(DbSetId ref) {
            this.ref = ref;
        }

        abstract protected DbBasicSelect getQuery();

        abstract protected T process(SasForwardCursor rs) throws IOException;

        @Override
        public T call() throws IOException {
            DbBasicSelect query = getQuery();
            Path conn = dbBean.getFile().toPath();
            try (SasStatement stmt = new SasStatement(sasquatch, conn)) {
                try (SasForwardCursor rs = stmt.executeQuery(query)) {
                    return process(rs);
                }
            }
        }
    }

    private static Map<String, String> toFilter(DbSetId id) {
        Map<String, String> result = new HashMap<>();
        for (int i = 0; i < id.getLevel(); i++) {
            result.put(id.getColumn(i), id.getValue(i));
        }
        return result;
    }
}
