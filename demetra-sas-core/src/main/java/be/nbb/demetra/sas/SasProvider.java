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

import ec.tss.ITsProvider;
import ec.tss.TsAsyncMode;
import ec.tss.tsproviders.DataSource;
import ec.tss.tsproviders.IFileLoader;
import ec.tss.tsproviders.db.DbAccessor;
import ec.tss.tsproviders.db.DbProvider;
import java.io.File;
import java.util.concurrent.atomic.AtomicReference;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.openide.util.lookup.ServiceProvider;
import org.slf4j.LoggerFactory;
import sasquatch.Sasquatch;

/**
 * https://github.com/BioStatMatt/sas7bdat/blob/master/R/sas7bdat.R
 *
 * @author Philippe Charles
 */
@Deprecated
@ServiceProvider(service = ITsProvider.class)
public final class SasProvider extends DbProvider<SasBean> implements IFileLoader {

    public static final String NAME = "SAS", VERSION = "20130925";

    private final AtomicReference<Sasquatch> sasquatch;
    private File[] paths;

    public SasProvider() {
        super(LoggerFactory.getLogger(SasProvider.class), NAME, TsAsyncMode.Once);
        this.sasquatch = new AtomicReference<>(Sasquatch.ofServiceLoader());
        this.paths = new File[0];
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
    protected DbAccessor<SasBean> loadFromBean(SasBean bean) throws Exception {
        return new SasAccessor(sasquatch.get(), bean).memoize();
    }

    @Override
    public DataSource encodeBean(Object bean) throws IllegalArgumentException {
        return support.checkBean(bean, SasBean.class).toDataSource(NAME, VERSION);
    }

    @Override
    public SasBean decodeBean(DataSource dataSource) throws IllegalArgumentException {
        support.check(dataSource);
        return new SasBean(dataSource);
    }

    @Override
    public String getDisplayName() {
        return "Sas";
    }

    @Override
    public SasBean newBean() {
        return new SasBean();
    }

    @Override
    public String getFileDescription() {
        return "Folder";
    }

    @Override
    public void setPaths(File[] paths) {
        this.paths = paths != null ? paths.clone() : new File[0];
        clearCache();
    }

    @Override
    public File[] getPaths() {
        return paths.clone();
    }

    @Override
    public boolean accept(File pathname) {
        return pathname.isDirectory();
    }
}
