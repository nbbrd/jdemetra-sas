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
package internal.demetra.sas7bdat;

import ec.tss.tsproviders.db.DbUtil;
import ec.tss.tsproviders.utils.IParser;
import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import javax.annotation.Nonnull;
import sasquatch.SasColumn;
import sasquatch.SasForwardCursor;

/**
 *
 * @author Philippe Charles
 * @param <T>
 */
public interface SasFunc<T> extends DbUtil.Func<SasForwardCursor, T, IOException> {

    @Nonnull
    public static SasFunc<String> onNull() {
        return NullFunc.INSTANCE;
    }

    @Nonnull
    public static SasFunc<String[]> onGetStringArray(int index, int length) {
        return o -> getStringArray(o, index, length);
    }

    @Nonnull
    public static SasFunc<String> onGetObjectToString(int index) {
        return o -> getObjectToString(o, index);
    }

    @Nonnull
    public static <X> SasFunc<X> compose(int index, IParser<X> parser) {
        return o -> getAndParse(o, index, parser);
    }

    @Nonnull
    public static SasFunc<LocalDate> onDate(SasForwardCursor rs, int index, IParser<LocalDate> dateParser) throws IOException {
        SasFunc<LocalDate> result = dateByDataType(rs.getColumns().get(index), index);
        return result != null ? result : compose(index, dateParser);
    }

    @Deprecated
    @Nonnull
    public static SasFunc<java.util.Date> onCalendar(SasForwardCursor rs, int index, IParser<java.util.Date> dateParser) throws IOException {
        SasFunc<java.util.Date> result = calendarByDataType(rs.getColumns().get(index), index);
        return result != null ? result : compose(index, dateParser);
    }

    @Nonnull
    public static SasFunc<Number> onNumber(SasForwardCursor rs, int index, IParser<Number> numberParser) throws IOException {
        SasFunc<Number> result = numberByDataType(rs.getColumns().get(index), index);
        return result != null ? result : compose(index, numberParser);
    }

    //<editor-fold defaultstate="collapsed" desc="Implementation details">
    static final class NullFunc implements SasFunc<String> {

        static final SasFunc<String> INSTANCE = new NullFunc();

        @Override
        public String apply(SasForwardCursor input) throws IOException {
            return null;
        }
    }

    static String[] getStringArray(SasForwardCursor rs, int index, int length) throws IOException {
        String[] result = new String[length];
        for (int i = 0; i < result.length; i++) {
            result[i] = rs.getValue(index + i).toString();
        }
        return result;
    }

    static String getObjectToString(SasForwardCursor rs, int index) throws IOException {
        return rs.getValue(index).toString();
    }

    static <X> X getAndParse(SasForwardCursor rs, int index, IParser<X> parser) throws IOException {
        return parser.parse(getObjectToString(rs, index));
    }

    static SasFunc<LocalDate> dateByDataType(SasColumn column, int index) {
        switch (column.getType()) {
            case DATE:
                return o -> o.getDate(index);
            case DATETIME:
                return o -> o.getDateTime(index).toLocalDate();
            case TIME:
                return o -> DATE_EPOCH;
        }
        return null;
    }

    @Deprecated
    static SasFunc<java.util.Date> calendarByDataType(SasColumn column, int index) {
        switch (column.getType()) {
            case DATE:
                return o -> Date.from(o.getDate(index).atStartOfDay(ZoneId.systemDefault()).toInstant());
            case DATETIME:
                return o -> Date.from(o.getDateTime(index).atZone(ZoneId.systemDefault()).toInstant());
            case TIME:
                return o -> Date.from(o.getTime(index).atDate(DATE_EPOCH).atZone(ZoneId.systemDefault()).toInstant());
        }
        return null;
    }

    static final LocalDate DATE_EPOCH = LocalDate.of(1960, 1, 1);

    static SasFunc<Number> numberByDataType(SasColumn column, int index) {
        switch (column.getType()) {
            case NUMERIC:
                return o -> o.getNumber(index);
        }
        return null;
    }
    //</editor-fold>
}
