/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query;

import org.apache.ignite.internal.processors.query.ddl.AbstractIndexOperation;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Active index operation.
 */
public class QueryIndexActiveOperation {
    /** Operation. */
    @GridToStringInclude
    private final AbstractIndexOperation op;

    /** Whether operation is accepted. */
    private boolean accepted;

    /**
     * Constructor.
     *
     * @param op Operation.
     */
    public QueryIndexActiveOperation(AbstractIndexOperation op) {
        this.op = op;
    }

    /**
     * @return Operation.
     */
    public AbstractIndexOperation operation() {
        return op;
    }

    /**
     * @return Whether operation is accepted.
     */
    public boolean accepted() {
        return accepted;
    }

    /**
     * Accept operation.
     */
    public void accept() {
        assert !accepted;

        accepted = true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(QueryIndexActiveOperation.class, this);
    }
}
