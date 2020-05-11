/*
 * Copyright 2018 National Bank of Belgium
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
package be.nbb.demetra.sas.file;

import ec.tss.ITsProvider;
import ec.tss.tsproviders.DataSet;
import ec.tss.tsproviders.DataSource;
import ec.tss.tsproviders.HasDataMoniker;
import ec.tss.tsproviders.HasDataSourceBean;
import ec.tss.tsproviders.HasDataSourceMutableList;
import ec.tss.tsproviders.HasFilePaths;
import ec.tss.tsproviders.IFileLoader;
import ec.tss.tsproviders.cube.CubeAccessor;
import ec.tss.tsproviders.cube.CubeId;
import ec.tss.tsproviders.cube.CubeSupport;
import ec.tss.tsproviders.cube.TableAsCubeAccessor;
import ec.tss.tsproviders.cube.TableDataParams;
import ec.tss.tsproviders.cursor.HasTsCursor;
import ec.tss.tsproviders.utils.DataSourcePreconditions;
import ec.tss.tsproviders.utils.IParam;
import ec.tstoolkit.utilities.GuavaCaches;
import internal.demetra.sas7bdat.SasTableAsCubeResource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.openide.util.lookup.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sasquatch.Sasquatch;

/**
 *
 * @author Philippe Charles
 */
@ServiceProvider(service = ITsProvider.class, supersedes = "be.nbb.demetra.sas.SasProvider")
public final class SasFileProvider implements IFileLoader {

    public static final String NAME = "SAS";

    private final AtomicReference<Sasquatch> sasquatch;

    @lombok.experimental.Delegate
    private final HasDataSourceMutableList mutableListSupport;

    @lombok.experimental.Delegate
    private final HasDataMoniker monikerSupport;

    @lombok.experimental.Delegate
    private final HasDataSourceBean<SasFileBean> beanSupport;

    @lombok.experimental.Delegate
    private final HasFilePaths filePathSupport;

    @lombok.experimental.Delegate(excludes = HasTsCursor.class)
    private final CubeSupport cubeSupport;

    @lombok.experimental.Delegate
    private final ITsProvider tsSupport;

    public SasFileProvider() {
        Logger logger = LoggerFactory.getLogger(NAME);
        ConcurrentMap<DataSource, CubeAccessor> cache = GuavaCaches.softValuesCacheAsMap();
        SasFileParam param = new SasFileParam.V1();

        this.sasquatch = new AtomicReference<>(Sasquatch.ofServiceLoader());

        this.mutableListSupport = HasDataSourceMutableList.of(NAME, logger, cache::remove);
        this.monikerSupport = HasDataMoniker.usingUri(NAME);
        this.beanSupport = HasDataSourceBean.of(NAME, param, param.getVersion());
        this.filePathSupport = HasFilePaths.of(cache::clear);
        this.cubeSupport = CubeSupport.of(new SasFileCubeResource(cache, param, filePathSupport, sasquatch));
        this.tsSupport = CubeSupport.asTsProvider(NAME, logger, cubeSupport, monikerSupport, cache::clear);
    }

    @NonNull
    public Sasquatch getSasquatch() {
        return sasquatch.get();
    }

    public void setSasquatch(@Nullable Sasquatch sasquatch) {
        Sasquatch old = this.sasquatch.get();
        if (this.sasquatch.compareAndSet(old, sasquatch != null ? sasquatch : Sasquatch.ofServiceLoader())) {
            clearCache();
        }
    }

    @Override
    public String getDisplayName() {
        return "Sas";
    }

    @Override
    public String getFileDescription() {
        return "Folder";
    }

    @Override
    public boolean accept(File pathname) {
        return pathname.isDirectory();
    }

    @lombok.AllArgsConstructor
    private static final class SasFileCubeResource implements CubeSupport.Resource {

        private final ConcurrentMap<DataSource, CubeAccessor> cache;
        private final SasFileParam param;
        private final HasFilePaths paths;
        private final AtomicReference<Sasquatch> sasquatch;

        @Override
        public CubeAccessor getAccessor(DataSource dataSource) throws IOException, IllegalArgumentException {
            DataSourcePreconditions.checkProvider(SasFileProvider.NAME, dataSource);
            CubeAccessor result = cache.get(dataSource);
            if (result == null) {
                result = load(dataSource);
                cache.put(dataSource, result);
            }
            return result;
        }

        @Override
        public IParam<DataSet, CubeId> getIdParam(DataSource dataSource) throws IOException, IllegalArgumentException {
            DataSourcePreconditions.checkProvider(SasFileProvider.NAME, dataSource);
            return param.getCubeIdParam(dataSource);
        }

        private CubeAccessor load(DataSource key) {
            SasFileBean bean = param.get(key);
            SasTableAsCubeResource result = SasTableAsCubeResource.create(sasquatch.get(), paths, bean.getFile(), bean.getTable(), bean.getDimColumns(), toDataParams(bean), bean.getObsGathering(), bean.getLabelColumn());
            return TableAsCubeAccessor.create(result).bulk(bean.getCacheDepth(), GuavaCaches.ttlCacheAsMap(bean.getCacheTtl()));
        }

        private static TableDataParams toDataParams(SasFileBean bean) {
            return TableDataParams.builder()
                    .periodColumn(bean.getPeriodColumn())
                    .valueColumn(bean.getValueColumn())
                    .versionColumn(bean.getVersionColumn())
                    .obsFormat(bean.getObsFormat())
                    .build();
        }
    }
}
