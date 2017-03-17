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

package org.apache.ignite.internal.jdbc2;

import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.GridTestUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.UUID;
import java.util.concurrent.Callable;

import static org.apache.ignite.IgniteJdbcDriver.CFG_URL_PREFIX;

/**
 * Test of cluster and JDBC driver with config that contains cache with POJO store and datasource bean.
 */
public class JdbcSpringSelfTest extends JdbcConnectionSelfTest {
    /** Grid count. */
    private static final int GRID_CNT = 2;

    /** Ignite configuration URL. */
    private static final String CFG_URL = "modules/clients/src/test/config/jdbc-config-cache-store.xml";

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsWithSpringCtx(GRID_CNT, false, CFG_URL);

        Class.forName("org.apache.ignite.IgniteJdbcDriver");
    }

    /**
     * @throws Exception If failed.
     */
    @Override public void testClientNodeId() throws Exception {
        IgniteEx client = (IgniteEx) startGridWithSpringCtx(getTestGridName(), true, CFG_URL);

        UUID clientId = client.localNode().id();

        final String url = CFG_URL_PREFIX + "nodeId=" + clientId + '@' + CFG_URL;

        GridTestUtils.assertThrows(
                log,
                new Callable<Object>() {
                    @Override public Object call() throws Exception {
                        try (Connection conn = DriverManager.getConnection(url)) {
                            return conn;
                        }
                    }
                },
                SQLException.class,
                "Failed to establish connection with node (is it a server node?): " + clientId
        );
    }
}
