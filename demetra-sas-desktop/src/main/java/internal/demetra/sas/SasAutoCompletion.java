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
package internal.demetra.sas;

import com.google.common.base.Strings;
import ec.tss.tsproviders.HasFilePaths;
import ec.util.completion.AutoCompletionSource;
import static ec.util.completion.AutoCompletionSource.Behavior.ASYNC;
import static ec.util.completion.AutoCompletionSource.Behavior.NONE;
import static ec.util.completion.AutoCompletionSource.Behavior.SYNC;
import ec.util.completion.ExtAutoCompletionSource;
import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import sasquatch.SasColumn;
import sasquatch.Sasquatch;

/**
 *
 * @author Philippe Charles
 */
@lombok.experimental.UtilityClass
public class SasAutoCompletion {

    public AutoCompletionSource onTables(HasFilePaths paths, Supplier<File> repo, ConcurrentMap cache) {
        return ExtAutoCompletionSource
                .builder(o -> loadTables(paths, repo))
                .behavior(o -> canLoadTables(repo) ? ASYNC : NONE)
                .postProcessor(SasAutoCompletion::filterAndSortTables)
                .cache(cache, o -> getTableCacheKey(repo), SYNC)
                .build();
    }

    public AutoCompletionSource onColumns(Sasquatch sasquatch, HasFilePaths paths, Supplier<File> repo, Supplier<String> table, ConcurrentMap cache) {
        return ExtAutoCompletionSource
                .builder(o -> loadColumns(sasquatch, paths, repo, table))
                .behavior(o -> canLoadColumns(repo, table) ? ASYNC : NONE)
                .postProcessor(SasAutoCompletion::filterAndSortColumns)
                .valueToString(SasColumn::getName)
                .cache(cache, o -> getColumnCacheKey(repo, table), SYNC)
                .build();
    }

    public String getDefaultColumnsAsString(Sasquatch sasquatch, HasFilePaths paths, Supplier<File> repo, Supplier<String> table, ConcurrentMap cache, CharSequence delimiter) throws IOException {
        String key = getColumnCacheKey(repo, table);
        List<SasColumn> columns = (List<SasColumn>) cache.get(key);
        if (columns == null) {
            columns = loadColumns(sasquatch, paths, repo, table);
            cache.put(key, columns);
        }
        return columns.stream()
                .sorted(Comparator.comparingInt(SasColumn::getOrder))
                .map(SasColumn::getName)
                .collect(Collectors.joining(delimiter));
    }

    private Path open(HasFilePaths paths, Supplier<File> repo) throws IOException {
        return paths.resolveFilePath(repo.get()).toPath();
    }

    private boolean canLoadTables(Supplier<File> repo) {
        return repo.get() != null && !repo.get().getPath().isEmpty();
    }

    private List<String> loadTables(HasFilePaths paths, Supplier<File> file) throws IOException {
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(open(paths, file), "*.sas7bdat")) {
            List<String> result = new ArrayList<>();
            for (Path path : ds) {
                result.add(path.getFileName().toString());
            }
            return result;
        }
    }

    private List<String> filterAndSortTables(List<String> values, String term) {
        return values.stream()
                .filter(ExtAutoCompletionSource.basicFilter(term)::test)
                .sorted()
                .collect(Collectors.toList());
    }

    private String getTableCacheKey(Supplier<File> file) {
        return file.get().getPath();
    }

    private boolean canLoadColumns(Supplier<File> file, Supplier<String> table) {
        return canLoadTables(file) && !Strings.isNullOrEmpty(table.get());
    }

    private List<SasColumn> loadColumns(Sasquatch sasquatch, HasFilePaths paths, Supplier<File> repo, Supplier<String> table) throws IOException {
        return sasquatch.readMetaData(open(paths, repo).resolve(table.get())).getColumns();
    }

    private List<SasColumn> filterAndSortColumns(List<SasColumn> values, String term) {
        Predicate<String> filter = ExtAutoCompletionSource.basicFilter(term);
        return values.stream()
                .filter(o -> filter.test(o.getName()) || filter.test(o.getType().name()) || filter.test(String.valueOf(o.getOrder())))
                .sorted(Comparator.comparing(SasColumn::getName))
                .collect(Collectors.toList());
    }

    private String getColumnCacheKey(Supplier<File> file, Supplier<String> table) {
        return getTableCacheKey(file) + "/ " + table.get();
    }
}
