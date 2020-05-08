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

import ec.tss.tsproviders.DataSource;
import ec.tss.tsproviders.IFileBean;
import ec.tss.tsproviders.db.DbBean;
import java.io.File;

/**
 *
 * @author Philippe Charles
 */
@Deprecated
public final class SasBean extends DbBean.BulkBean implements IFileBean {

    public SasBean() {
    }

    public SasBean(DataSource id) {
        super(id);
    }

    @Override
    public File getFile() {
        return new File(getDbName());
    }

    @Override
    public void setFile(File file) {
        setDbName(file.getPath());
    }

    public File getTableFile() {
        return new File(getDbName(), getTableName());
    }
}
