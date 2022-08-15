package com.github.isuhorukov.jdbc.bridge;

import org.postgresql.pljava.ResultSetHandle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class Query implements ResultSetHandle {
    private Connection connection;
    private PreparedStatement preparedStatement;
    private String sql;
    private Object[] params;

    public Query(Connection connection,String sql, Object... params) {
        this.connection = connection;
        this.sql = sql;
        this.params = params;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        preparedStatement = connection.prepareStatement(sql);
        if(params!=null && params.length>0){
            for(int idx=0;idx<params.length;idx++){
                preparedStatement.setObject(idx+1, params[idx]);
            }
        }
        return preparedStatement.executeQuery();
    }

    @Override
    public void close() throws SQLException {
        try {
            preparedStatement.close();
        } finally {
            connection.close();
        }
    }
}
