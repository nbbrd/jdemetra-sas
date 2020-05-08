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

import be.nbb.demetra.sas.*;
import _test.SasReaderOverCsv;
import ec.tss.TsCollectionInformation;
import ec.tss.TsInformationType;
import ec.tss.TsMoniker;
import ec.tss.tsproviders.DataSet;
import ec.tss.tsproviders.DataSource;
import ec.tss.tsproviders.IDataSourceLoaderAssert;
import ec.tss.tsproviders.IDataSourceProvider;
import ec.tss.tsproviders.IFileLoaderAssert;
import ec.tstoolkit.timeseries.simplets.TsDomain;
import ec.tstoolkit.timeseries.simplets.TsFrequency;
import ec.tstoolkit.timeseries.simplets.TsPeriod;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import static org.assertj.core.api.Assertions.assertThat;
import org.junit.Test;
import sasquatch.Sasquatch;

/**
 *
 * @author Philippe Charles
 */
public class SasFileProviderTest {

    @Test
    public void testEquivalence() throws IOException {
        IDataSourceLoaderAssert.assertThat(getProvider())
                .isEquivalentTo(getPreviousProvider(), o -> o.encodeBean(getPreviousBean(o)));
    }

    @Test
    public void testTspCompliance() {
        IDataSourceLoaderAssert.assertCompliance(SasFileProviderTest::getProvider, SasFileProviderTest::getBean);
    }

    @Test
    public void testMonikerUri() {
        String uri = "demetra://tsprovider/SAS/20130925/SERIES?aggregationType=Last&cacheDepth=2&cacheTtl=1000&cleanMissing=false&datePattern=dd%2FMM%2Fyyyy&dbName=mydb&dimColumns=Sector%2C+Region&frequency=Monthly&labelColumn=Title&locale=fr&numberPattern=%23.%23&periodColumn=Table2.Period&tableName=Table2&valueColumn=Rate&versionColumn=Version#Region=Belgium&Sector=Industry";

        DataSource source = DataSource.builder("SAS", "20130925")
                .put("dbName", "mydb")
                .put("tableName", "Table2")
                .put("dimColumns", "Sector, Region")
                .put("periodColumn", "Table2.Period")
                .put("valueColumn", "Rate")
                .put("locale", "fr")
                .put("datePattern", "dd/MM/yyyy")
                .put("numberPattern", "#.#")
                .put("versionColumn", "Version")
                .put("labelColumn", "Title")
                .put("frequency", "Monthly")
                .put("aggregationType", "Last")
                .put("cleanMissing", "false")
                .put("cacheTtl", "1000")
                .put("cacheDepth", "2")
                .build();

        DataSet expected = DataSet.builder(source, DataSet.Kind.SERIES)
                .put("Sector", "Industry")
                .put("Region", "Belgium")
                .build();

        try (IDataSourceProvider p = new SasFileProvider()) {
            assertThat(p.toDataSet(new TsMoniker("SAS", uri))).isEqualTo(expected);
        }
    }

    @Test
    public void testContent() {
        try (SasFileProvider p = getProvider()) {
            TsCollectionInformation allData = new TsCollectionInformation();
            allData.moniker = p.toMoniker(p.encodeBean(getBean(p)));
            allData.type = TsInformationType.All;

            assertThat(p.get(allData)).isTrue();
            assertThat(allData.items)
                    .hasSize(12)
                    .element(0)
                    .satisfies(o -> {
                        assertThat(o.name).isEqualTo("Monthly, Chrome");
                        assertThat(o.data.getDomain()).isEqualTo(new TsDomain(new TsPeriod(TsFrequency.Monthly, 2008, 6), 41));
                        assertThat(o.data.internalStorage()).containsExactly(0, 0, 1.03, 1.02, 0.93, 1.21, 1.38, 1.52, 1.73, 2.07, 2.42, 2.82, 3.01, 3.38, 3.69, 4.17, 4.66, 5.45, 6.04, 6.72, 7.29, 8.06, 8.61, 9.24, 9.88, 10.76, 11.54, 12.39, 13.35, 14.85, 15.68, 16.54, 17.37, 18.29, 19.36, 20.65, 22.14, 23.16, 23.61, 25, 25.65);
                    });
        }
    }

    private static SasFileProvider getProvider() {
        SasFileProvider result = new SasFileProvider();
        result.setSasquatch(Sasquatch.of(new SasReaderOverCsv()));
        return result;
    }

    private static SasProvider getPreviousProvider() {
        SasProvider result = new SasProvider();
        result.setSasquatch(Sasquatch.of(new SasReaderOverCsv()));
        return result;
    }

    private static SasFileBean getBean(SasFileProvider o) {
        SasFileBean result = o.newBean();
        result.setFile(RESOURCE.getParentFile());
        result.setTable(RESOURCE.getName());
        result.setDimColumns(Arrays.asList("Freq,Browser"));
        result.setPeriodColumn("Period");
        result.setValueColumn("MarketShare");
        result.setCacheDepth(0);
        return result;
    }

    private static SasBean getPreviousBean(SasProvider o) {
        SasBean result = o.newBean();
        result.setFile(RESOURCE.getParentFile());
        result.setTableName(RESOURCE.getName());
        result.setDimColumns("Freq,Browser");
        result.setPeriodColumn("Period");
        result.setValueColumn("MarketShare");
        result.setCacheDepth(0);
        return result;
    }

    private static final File RESOURCE = IFileLoaderAssert.urlAsFile(SasAccessorTest.class.getResource("/Top5Browsers-Table-Top5.csv"));
}
