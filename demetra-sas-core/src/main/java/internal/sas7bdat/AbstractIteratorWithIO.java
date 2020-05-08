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
package internal.sas7bdat;

import ec.tss.tsproviders.utils.IteratorWithIO;
import java.io.IOException;
import java.util.NoSuchElementException;

/**
 *
 * @author Philippe Charles
 */
abstract class AbstractIteratorWithIO<E> implements IteratorWithIO<E> {

    private State state = State.NOT_READY;

    private enum State {
        READY, NOT_READY, DONE, FAILED
    }
    private E next;

    protected abstract E computeNext() throws IOException;

    protected final E endOfData() {
        state = State.DONE;
        return null;
    }

    @Override
    public final boolean hasNext() throws IOException {
        if (state == State.FAILED) {
            throw new IllegalArgumentException();
        }
        switch (state) {
            case DONE:
                return false;
            case READY:
                return true;
            default:
        }
        return tryToComputeNext();
    }

    private boolean tryToComputeNext() throws IOException {
        state = State.FAILED;
        next = computeNext();
        if (state != State.DONE) {
            state = State.READY;
            return true;
        }
        return false;
    }

    @Override
    public final E next() throws IOException {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        state = State.NOT_READY;
        E result = next;
        next = null;
        return result;
    }
}
