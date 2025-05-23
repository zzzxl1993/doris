// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import org.junit.Assert;

suite("test_select_count_auth","p0,auth") {
    String suiteName = "test_select_count_auth"
    String user = "${suiteName}_user"
    String pwd = 'C123_567p'
    try_sql("DROP USER ${user}")
    sql """CREATE USER '${user}' IDENTIFIED BY '${pwd}'"""

    //cloud-mode
    if (isCloudMode()) {
        def clusters = sql " SHOW CLUSTERS; "
        assertTrue(!clusters.isEmpty())
        def validCluster = clusters[0][0]
        sql """GRANT USAGE_PRIV ON CLUSTER `${validCluster}` TO ${user}""";
    }

    sql """grant select_priv on regression_test to ${user}"""

    connect(user, "${pwd}", context.config.jdbcUrl) {
        test {
              sql """
                  select count(*) from __internal_schema.audit_log;
              """
              exception "denied"
        }
        test {
              sql """
                  select count(1) from __internal_schema.audit_log;
              """
              exception "denied"
        }
        test {
              sql """
                  select count(query_id) from __internal_schema.audit_log;
              """
              exception "denied"
        }
    }

    sql """grant select_priv(query_id) on __internal_schema.audit_log to ${user}"""

    connect(user, "${pwd}", context.config.jdbcUrl) {
        test {
              sql """
                  select count(*) from __internal_schema.audit_log;
              """
              exception "denied"
        }
        test {
              sql """
                  select count(1) from __internal_schema.audit_log;
              """
              exception "denied"
        }
      sql """
          select count(query_id) from __internal_schema.audit_log;
      """
    }

    sql """grant select_priv on __internal_schema.audit_log to ${user}"""

    connect(user, "${pwd}", context.config.jdbcUrl) {
        sql """
              select count(*) from __internal_schema.audit_log;
          """
        sql """
              select count(1) from __internal_schema.audit_log;
          """
          sql """
              select count(query_id) from __internal_schema.audit_log;
          """
    }

    try_sql("DROP USER ${user}")
}
