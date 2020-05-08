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
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.Objects;
import javax.annotation.Nonnull;

import sasquatch.SasColumn;
import sasquatch.SasForwardCursor;

/**
 * @param <T>
 * @author Philippe Charles
 */
public interface SasFunc<T> extends DbUtil.Func<SasForwardCursor, T, IOException> {

    @Nonnull
    static SasFunc<String> onNull() {
        return NullFunc.INSTANCE;
    }

    @Nonnull
    static SasFunc<String[]> onGetStringArray(int index, int length) {
        return o -> getStringArray(o, index, length);
    }

    @Nonnull
    static SasFunc<String> onGetObjectToString(int index) {
        return o -> getObjectToString(o, index);
    }

    @Nonnull
    static <X> SasFunc<X> compose(int index, IParser<X> parser) {
        return o -> getAndParse(o, index, parser);
    }

    @Nonnull
    static SasFunc<LocalDate> onDate(SasForwardCursor rs, int index, IParser<LocalDate> dateParser) throws IOException {
        SasFunc<LocalDate> result = dateByDataType(rs.getColumns().get(index), index);
        return result != null ? result : compose(index, dateParser);
    }

    @Deprecated
    @Nonnull
    static SasFunc<java.util.Date> onCalendar(SasForwardCursor rs, int index, IParser<java.util.Date> dateParser) throws IOException {
        SasFunc<java.util.Date> result = calendarByDataType(rs.getColumns().get(index), index);
        return result != null ? result : compose(index, dateParser);
    }

    @Nonnull
    static SasFunc<Number> onNumber(SasForwardCursor rs, int index, IParser<Number> numberParser) throws IOException {
        SasFunc<Number> result = numberByDataType(rs.getColumns().get(index), index);
        return result != null ? result : compose(index, numberParser);
    }

    //<editor-fold defaultstate="collapsed" desc="Implementation details">
    final class NullFunc implements SasFunc<String> {

        static final SasFunc<String> INSTANCE = new NullFunc();

        @Override
        public String apply(SasForwardCursor input) {
            return null;
        }
    }

    static String[] getStringArray(SasForwardCursor rs, int index, int length) throws IOException {
        String[] result = new String[length];
        for (int i = 0; i < result.length; i++) {
            result[i] = getObjectToString(rs, index + i);
        }
        return result;
    }

    static String getObjectToString(SasForwardCursor rs, int index) throws IOException {
        Object value = rs.getValue(index);
        return value != null ? value.toString() : null;
    }

    static <X> X getAndParse(SasForwardCursor rs, int index, IParser<X> parser) throws IOException {
        return parser.parse(getObjectToString(rs, index));
    }

    static SasFunc<LocalDate> dateByDataType(SasColumn column, int index) {
        switch (column.getType()) {
            case DATE:
                return o -> o.getDate(index);
            case DATETIME:
                return o -> {
                    LocalDateTime value = o.getDateTime(index);
                    return value != null ? value.toLocalDate() : null;
                };
            case TIME:
                return o -> DATE_EPOCH;
        }
        return null;
    }

    @Deprecated
    static SasFunc<java.util.Date> calendarByDataType(SasColumn column, int index) {
        ZoneId zoneId = ZoneId.systemDefault();
        switch (column.getType()) {
            case DATE:
                return o -> {
                    LocalDate value = o.getDate(index);
                    return value != null ? Date.from(value.atStartOfDay(zoneId).toInstant()) : null;
                };
            case DATETIME:
                return o -> {
                    LocalDateTime value = o.getDateTime(index);
                    return value != null ? Date.from(value.atZone(zoneId).toInstant()) : null;
                };
            case TIME:
                return o -> {
                    LocalTime value = o.getTime(index);
                    return value != null ? Date.from(value.atDate(DATE_EPOCH).atZone(zoneId).toInstant()) : null;
                };
        }
        return null;
    }

    LocalDate DATE_EPOCH = LocalDate.of(1960, 1, 1);

    static SasFunc<Number> numberByDataType(SasColumn column, int index) {
        switch (column.getType()) {
            case NUMERIC:
                return o -> o.getNumber(index);
        }
        return null;
    }
    //</editor-fold>
}
