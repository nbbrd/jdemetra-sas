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

import _test.SasReaderOverCsv;
import ec.tss.tsproviders.IFileLoaderAssert;
import ec.tss.tsproviders.db.DbSeries;
import java.io.File;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import sasquatch.Sasquatch;

/**
 *
 * @author Philippe Charles
 */
public class SasAccessorTest {

    private static final File RESOURCE = IFileLoaderAssert.urlAsFile(SasAccessorTest.class.getResource("/Top5Browsers-Table-Top5.csv"));

    SasBean createBean() {
        SasBean result = new SasBean();
        result.setDbName(RESOURCE.getParent());
        result.setTableName(RESOURCE.getName());
        result.setDimColumns("Freq,Browser");
        result.setPeriodColumn("Period");
        result.setValueColumn("MarketShare");
        result.setCacheDepth(0);
        return result;
    }

    @Test
    public void test() throws Exception {
        SasAccessor accessor = new SasAccessor(Sasquatch.of(new SasReaderOverCsv()), createBean());
        List<DbSeries> allSeries = accessor.getAllSeriesWithData();
        Assertions.assertEquals(12, allSeries.size());
    }
}
