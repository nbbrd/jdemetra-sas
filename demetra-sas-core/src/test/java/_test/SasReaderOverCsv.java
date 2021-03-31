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
package _test;

import nbbrd.picocsv.Csv;
import sasquatch.*;
import sasquatch.spi.SasFeature;
import sasquatch.spi.SasReader;
import sasquatch.util.SasCursors;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.NumberFormat;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author Philippe Charles
 */
public final class SasReaderOverCsv implements SasReader {

    @Override
    public String getName() {
        return "SasReaderOverCsv";
    }

    @Override
    public int getCost() {
        return BASIC_SUPPORT;
    }

    @Override
    public boolean isAvailable() {
        return true;
    }

    @Override
    public Set<SasFeature> getFeatures() {
        return EnumSet.noneOf(SasFeature.class);
    }

    @Override
    public SasMetaData readMetaData(Path file) throws IOException {
        return getMeta();
    }

    @Override
    public SasForwardCursor readForward(Path file) throws IOException {
        List<Object[]> data = readDataFromCsv(file);
        return SasCursors.forwardOf(getMeta(), data);
    }

    @Override
    public SasScrollableCursor readScrollable(Path file) throws IOException {
        List<Object[]> data = readDataFromCsv(file);
        return SasCursors.scrollableOf(getMeta(), data);
    }

    @Override
    public SasSplittableCursor readSplittable(Path file) throws IOException {
        List<Object[]> data = readDataFromCsv(file);
        return SasCursors.splittableOf(getMeta(), data);
    }

    private List<Object[]> readDataFromCsv(Path file) throws IOException {
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("d/MM/yyyy");
        NumberFormat numberFormat = NumberFormat.getInstance(Locale.FRENCH);

        List<Object[]> result = new ArrayList<>();
        Csv.Format format = Csv.Format.builder().delimiter(';').build();
        Csv.ReaderOptions options = Csv.ReaderOptions.builder().lenientSeparator(true).build();
        try (Csv.Reader reader = Csv.Reader.of(format, options, Files.newBufferedReader(file, StandardCharsets.UTF_8), Csv.DEFAULT_CHAR_BUFFER_SIZE)) {
            reader.readLine();
            while (reader.readLine()) {
                Object[] fields = new Object[4];
                fields[0] = reader.readField() ? reader.toString() : null;
                fields[1] = reader.readField() ? reader.toString() : null;
                fields[2] = dateFormatter.parse(reader.readField() ? reader : null, LocalDate::from);
                try {
                    fields[3] = numberFormat.parse(reader.readField() ? reader.toString() : null);
                } catch (ParseException ex) {
                    throw new IOException(ex);
                }
                result.add(fields);
            }
        }
        return result;
    }

    private SasMetaData getMeta() {
        return SasMetaData
                .builder()
                .column(SasColumn.builder().order(0).name("Freq").type(SasColumnType.CHARACTER).build())
                .column(SasColumn.builder().order(1).name("Browser").type(SasColumnType.CHARACTER).build())
                .column(SasColumn.builder().order(2).name("Period").type(SasColumnType.DATE).build())
                .column(SasColumn.builder().order(3).name("MarketShare").type(SasColumnType.NUMERIC).build())
                .rowCount(330)
                .build();
    }
}
