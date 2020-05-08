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

import ec.nbdemetra.db.DbProviderBuddy;
import ec.nbdemetra.ui.properties.FileLoaderFileFilter;
import ec.nbdemetra.ui.properties.NodePropertySetBuilder;
import ec.nbdemetra.ui.tsproviders.IDataSourceProviderBuddy;
import ec.tss.tsproviders.IFileLoader;
import ec.tss.tsproviders.TsProviders;
import ec.tstoolkit.utilities.GuavaCaches;
import ec.util.completion.AutoCompletionSource;
import ec.util.completion.AutoCompletionSources;
import internal.demetra.sas.SasAutoCompletion;
import internal.demetra.sas.SasColumnRenderer;
import java.awt.Image;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ConcurrentMap;
import javax.swing.ListCellRenderer;
import org.openide.util.ImageUtilities;
import org.openide.util.NbBundle;
import org.openide.util.lookup.ServiceProvider;

/**
 *
 * @author Philippe Charles
 */
@Deprecated
@ServiceProvider(service = IDataSourceProviderBuddy.class)
public class SasProviderBuddy extends DbProviderBuddy<SasBean> {

    private final ConcurrentMap autoCompletionCache;

    public SasProviderBuddy() {
        this.autoCompletionCache = GuavaCaches.ttlCacheAsMap(Duration.ofMinutes(1));
    }

    @Override
    protected boolean isFile() {
        return true;
    }

    @Override
    public String getProviderName() {
        return SasProvider.NAME;
    }

    @Override
    public Image getIcon(int type, boolean opened) {
        return ImageUtilities.loadImage("be/nbb/demetra/sas/sas7bdat.png", true);
    }

    @NbBundle.Messages({
        "bean.file.display=Datasets repository",
        "bean.file.description=The path to the datasets repository."})
    @Override
    protected NodePropertySetBuilder withFileName(NodePropertySetBuilder b, SasBean bean) {
        IFileLoader loader = TsProviders.lookup(IFileLoader.class, getProviderName()).get();
        return b.withFile()
                .select(bean, "file")
                .filterForSwing(new FileLoaderFileFilter(loader))
                .paths(loader.getPaths())
                .directories(true)
                .display(Bundle.bean_file_display())
                .description(Bundle.bean_file_description())
                .add();
    }

    @Override
    protected AutoCompletionSource getTableSource(SasBean bean) {
        return lookupProvider()
                .map(o -> SasAutoCompletion.onTables(o, bean::getFile, autoCompletionCache))
                .orElseGet(AutoCompletionSources::empty);
    }

    @Override
    protected AutoCompletionSource getColumnSource(SasBean bean) {
        return lookupProvider()
                .map(o -> SasAutoCompletion.onColumns(o.getSasquatch(), o, bean::getFile, bean::getTableName, autoCompletionCache))
                .orElseGet(AutoCompletionSources::empty);
    }

    @Override
    protected ListCellRenderer getColumnRenderer(SasBean bean) {
        return new SasColumnRenderer();
    }

    private static Optional<SasProvider> lookupProvider() {
        return TsProviders.lookup(SasProvider.class, SasProvider.NAME).toJavaUtil();
    }
}
