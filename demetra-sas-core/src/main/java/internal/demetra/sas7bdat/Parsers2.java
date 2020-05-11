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
package internal.demetra.sas7bdat;

import ec.tss.tsproviders.utils.DataFormat;
import ec.tss.tsproviders.utils.IParser;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.format.FormatStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalQuery;
import java.util.Locale;
import java.util.Objects;

/**
 *
 * @author Philippe Charles
 */
@lombok.experimental.UtilityClass
class Parsers2 {

    @NonNull
    public <T> IParser<T> dateTimeParser(@NonNull DataFormat obsFormat, @NonNull TemporalQuery<T> query) {
        try {
            return onDateTimeFormatter(newDateTimeFormatter(obsFormat.getDatePattern(), obsFormat.getLocale()), query);
        } catch (IllegalArgumentException ex) {
            return Parsers2::parseNull;
        }
    }

    private <T> IParser<T> onDateTimeFormatter(DateTimeFormatter formatter, TemporalQuery<T> query) {
        Objects.requireNonNull(formatter);
        Objects.requireNonNull(query);
        return o -> parseTemporalAccessor(formatter, query, o);
    }

    private <T> T parseNull(CharSequence input) {
        Objects.requireNonNull(input);
        return null;
    }

    private <T> T parseTemporalAccessor(DateTimeFormatter formatter, TemporalQuery<T> query, CharSequence input) {
        try {
            return formatter.parse(input, query);
        } catch (DateTimeParseException ex) {
            return null;
        }
    }

    private DateTimeFormatter newDateTimeFormatter(String datePattern, Locale locale) throws IllegalArgumentException {
        DateTimeFormatterBuilder result = new DateTimeFormatterBuilder()
                .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
                .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
                .parseDefaulting(ChronoField.HOUR_OF_DAY, 0)
                .parseDefaulting(ChronoField.MINUTE_OF_DAY, 0)
                .parseDefaulting(ChronoField.SECOND_OF_DAY, 0);

        if (!datePattern.isEmpty()) {
            result.appendPattern(datePattern);
        } else {
            result.append(DateTimeFormatter.ofLocalizedDate(FormatStyle.MEDIUM));
        }

        return locale != null
                ? result.toFormatter(locale)
                : result.toFormatter();
    }
}
