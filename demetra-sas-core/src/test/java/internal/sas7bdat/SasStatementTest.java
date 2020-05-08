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

import _test.SasReaderOverCsv;
import be.nbb.demetra.sas.SasAccessorTest;
import ec.tss.tsproviders.IFileLoaderAssert;
import internal.xdb.DbBasicSelect;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import static org.junit.Assert.*;
import org.junit.Test;
import sasquatch.SasForwardCursor;
import sasquatch.Sasquatch;

/**
 *
 * @author Philippe Charles
 */
public class SasStatementTest {

    @Test
    public void testSelect() throws IOException {
        DbBasicSelect c_bic = DbBasicSelect.from(RESOURCE.getName()).select("Freq").build();
        execute(c_bic, rs -> {
            assertEquals(1, rs.getColumns().size());
            assertEquals("Freq", rs.getColumns().get(0).getName());

            int cpt = 0;
            while (rs.next()) {
                cpt++;
            }
            assertEquals(330, cpt);
        });

        DbBasicSelect c_r_ecb = DbBasicSelect.from(RESOURCE.getName()).select("Browser").build();
        execute(c_r_ecb, rs -> {
            assertEquals(1, rs.getColumns().size());
            assertEquals("Browser", rs.getColumns().get(0).getName());

            int cpt = 0;
            while (rs.next()) {
                cpt++;
            }
            assertEquals(330, cpt);
        });

        DbBasicSelect both1 = DbBasicSelect.from(RESOURCE.getName()).select("Browser", "Freq").build();
        execute(both1, rs -> {
            assertEquals(2, rs.getColumns().size());
            assertEquals("Browser", rs.getColumns().get(0).getName());
            assertEquals("Freq", rs.getColumns().get(1).getName());

            int cpt = 0;
            while (rs.next()) {
                cpt++;
            }
            assertEquals(330, cpt);
        });

        DbBasicSelect both2 = DbBasicSelect.from(RESOURCE.getName()).select("Freq", "Browser").build();
        execute(both2, rs -> {
            assertEquals(2, rs.getColumns().size());
            assertEquals("Freq", rs.getColumns().get(0).getName());
            assertEquals("Browser", rs.getColumns().get(1).getName());

            int cpt = 0;
            while (rs.next()) {
                cpt++;
            }
            assertEquals(330, cpt);
        });
    }

    @Test
    public void testDistinct() throws IOException {
        DbBasicSelect c_bic = DbBasicSelect.from(RESOURCE.getName()).distinct(true).select("Freq").build();
        execute(c_bic, rs -> {
            assertEquals(1, rs.getColumns().size());
            assertEquals("Freq", rs.getColumns().get(0).getName());

            LinkedList<String> rows = new LinkedList<>();
            while (rs.next()) {
                rows.add((String) rs.getValue(0));
            }
            assertEquals(2, rows.size());
            assertEquals("Monthly", rows.getFirst());
            assertEquals("Quarterly", rows.getLast());
        });

        DbBasicSelect c_r_ecb = DbBasicSelect.from(RESOURCE.getName()).distinct(true).select("Browser").build();
        execute(c_r_ecb, rs -> {
            assertEquals(1, rs.getColumns().size());
            assertEquals("Browser", rs.getColumns().get(0).getName());

            LinkedList<String> result = new LinkedList<>();
            while (rs.next()) {
                result.add((String) rs.getValue(0));
            }
            assertEquals(6, result.size());
            assertEquals("Chrome", result.getFirst());
            assertEquals("Safari", result.getLast());
        });
    }

    @Test
    public void testOrder() throws IOException {
        DbBasicSelect select = DbBasicSelect.from(RESOURCE.getName()).distinct(true).select("Freq", "Browser").orderBy("Freq", "Browser").build();
        execute(select, rs -> {
            assertEquals(2, rs.getColumns().size());
            assertEquals("Freq", rs.getColumns().get(0).getName());
            assertEquals("Browser", rs.getColumns().get(1).getName());

            LinkedList<String> result = new LinkedList<>();
            while (rs.next()) {
                result.add(rs.getValue(0) + " - " + rs.getValue(1));
            }
            assertEquals(12, result.size());
            assertEquals("Monthly - Chrome", result.getFirst());
            assertEquals("Quarterly - Safari", result.getLast());
        });
    }

    @Test
    public void testFilter() throws IOException {
        Map<String, String> filter = new HashMap<>();
        filter.put("Browser", "IE");
        DbBasicSelect select = DbBasicSelect.from(RESOURCE.getName()).distinct(true).select("Freq", "Browser").filter(filter).build();
        execute(select, rs -> {
            assertEquals(2, rs.getColumns().size());
            assertEquals("Freq", rs.getColumns().get(0).getName());
            assertEquals("Browser", rs.getColumns().get(1).getName());

            LinkedList<String> result = new LinkedList<>();
            while (rs.next()) {
                result.add(rs.getValue(0) + " - " + rs.getValue(1));
            }
            assertEquals(2, result.size());
            assertEquals("Monthly - IE", result.getFirst());
            assertEquals("Quarterly - IE", result.getLast());
        });
    }

    private interface Callback {

        void process(SasForwardCursor rs) throws IOException;
    }

    private static void execute(DbBasicSelect query, Callback action) throws IOException {
        Path conn = RESOURCE.getParentFile().toPath();
        Sasquatch sasquatch = Sasquatch.of(new SasReaderOverCsv());
        try ( SasStatement stmt = new SasStatement(sasquatch, conn)) {
            try ( SasForwardCursor rs = stmt.executeQuery(query)) {
                action.process(rs);
            }
        }
    }

    private static final File RESOURCE = IFileLoaderAssert.urlAsFile(SasAccessorTest.class.getResource("/Top5Browsers-Table-Top5.csv"));
}
