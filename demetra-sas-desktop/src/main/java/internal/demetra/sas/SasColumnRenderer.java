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

import ec.nbdemetra.db.DbColumnListCellRenderer;
import ec.nbdemetra.db.DbIcon;
import javax.swing.Icon;
import sasquatch.SasColumn;

/**
 *
 * @author Philippe Charles
 */
public final class SasColumnRenderer extends DbColumnListCellRenderer<SasColumn> {

    @Override
    protected String getName(SasColumn value) {
        return value.getName();
    }

    @Override
    protected String getTypeName(SasColumn value) {
        return value.getType().name();
    }

    @Override
    protected Icon getTypeIcon(SasColumn value) {
        switch (value.getType()) {
            case CHARACTER:
                return DbIcon.DATA_TYPE_STRING;
            case NUMERIC:
                return DbIcon.DATA_TYPE_DOUBLE;
            case DATE:
            case DATETIME:
            case TIME:
                return DbIcon.DATA_TYPE_DATETIME;
        }
        return null;
    }
}
