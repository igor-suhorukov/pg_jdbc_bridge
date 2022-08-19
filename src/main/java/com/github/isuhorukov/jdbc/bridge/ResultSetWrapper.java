package com.github.isuhorukov.jdbc.bridge;

import lombok.experimental.Delegate;

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * As workaround for pljava getObject mapper
 */
public class ResultSetWrapper implements ResultSet {

    private interface GetObject {
        Object getObject(int columnIndex) throws SQLException;
    }

    @Delegate(excludes = GetObject.class)
    private ResultSet delegate;

    public ResultSetWrapper(ResultSet delegate) {
        this.delegate = delegate;
    }

    public Object getObject(int columnIndex) throws SQLException {
        Object object = delegate.getObject(columnIndex);
        if (object == null || !Array.class.isAssignableFrom(object.getClass())) {
            return object;
        } else {
            Array array = (Array) object;
            return array.getArray();
        }
    }
}
