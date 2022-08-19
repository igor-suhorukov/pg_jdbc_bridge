package com.github.isuhorukov.jdbc.bridge;

import org.postgresql.pljava.ResultSetHandle;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class JdbcQuery {

    public static ResultSetHandle selectWithParams(String driverClass, String url, String user, String password,
                                                   String selectSQL, Object... params) throws SQLException {
        JdbcUtils.initDriver(driverClass);
        Connection connection = DriverManager.getConnection(url, user, password);
        return new Query(connection, selectSQL, params);
    }

    public static ResultSetHandle select(String driverClass, String url, String user, String password, String selectSQL) throws SQLException {
        return selectWithParams(driverClass, url, user, password, selectSQL);
    }
}
