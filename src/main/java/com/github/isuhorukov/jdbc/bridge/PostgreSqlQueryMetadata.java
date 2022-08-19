package com.github.isuhorukov.jdbc.bridge;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class PostgreSqlQueryMetadata {

    protected static final Map<Class<?>, String> JAVA_TO_PG_MAPPING = Map.ofEntries(
            Map.entry(Long.class,"bigint"),
            Map.entry(Integer.class,"integer"),
            Map.entry(Short.class,"smallint"),
            Map.entry(Boolean.class, "boolean"),
            Map.entry(Float.class, "real"),
            Map.entry(BigDecimal.class, "numeric"),
            Map.entry(BigInteger.class, "numeric"),
            Map.entry(Double.class, "double precision"),
            Map.entry(Date.class, "timestamp"),
            Map.entry(String.class,"text"),
            Map.entry(Map.class,"hstore"),
            Map.entry(HashMap.class,"hstore"));

    protected static final Map<Integer, String> TYPE_MAPPING = Map.ofEntries(
            Map.entry(java.sql.Types.BIGINT, "bigint"),
            Map.entry(java.sql.Types.BIT, "boolean"),
            Map.entry(java.sql.Types.BOOLEAN, "boolean"),
            Map.entry(java.sql.Types.DATE, "date"),
            Map.entry(java.sql.Types.TIME, "time"),
            Map.entry(java.sql.Types.TIMESTAMP, "timestamp"),
            Map.entry(java.sql.Types.DECIMAL, "numeric"),
            Map.entry(java.sql.Types.BINARY, "bytea"),
            Map.entry(java.sql.Types.REAL, "real"),
            Map.entry(java.sql.Types.DOUBLE, "double precision"),
            Map.entry(java.sql.Types.INTEGER, "integer"),
            Map.entry(java.sql.Types.SMALLINT, "smallint"),
            Map.entry(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, "timestamp with time zone"),
            Map.entry(java.sql.Types.TINYINT, "smallint"),// pguint and int1/uint1
            Map.entry(java.sql.Types.VARBINARY, "bytea"),
            Map.entry(java.sql.Types.LONGVARBINARY, "bytea"),
            Map.entry(java.sql.Types.VARCHAR, "text"),
            Map.entry(java.sql.Types.LONGVARCHAR, "text"));

    public static String[] getQueryMetadata(String driverClass,
                                            String url, String user, String password, String sql) throws SQLException {
        JdbcUtils.initDriver(driverClass);
        try (Connection connection = DriverManager.getConnection(url, user, password)){
            try (ResultSet resultSet = connection.createStatement().executeQuery(sql)){
                return getQueryMetadata(resultSet);
            }
        }
    }

    private static String[] getQueryMetadata(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        Class<?>[] columns = new Class<?>[columnCount];
        String[] pgObjectType = new String[columnCount];
        boolean inspectComplexTypes = isInspectComplexTypes(metaData, columnCount);
        if(inspectComplexTypes){
            while (resultSet.next()) {
                for(int idx=1; idx<=columnCount; idx++){
                    Object object = resultSet.getObject(idx);
                    if(columns[idx-1]==null && object!=null){
                        columns[idx-1] = object.getClass();
                        if(Array.class.isAssignableFrom(columns[idx-1])){
                            Array array = (Array) object;
                            Object array1 = array.getArray();
                            columns[idx-1] = array1.getClass().getComponentType();
                        } else
                        if("org.postgresql.util.PGobject".equals(columns[idx-1].getName())){
                            pgObjectType[idx-1] = getPgObjectSubtype(columns[idx - 1], object);
                        }
                    }
                }
                if(Arrays.stream(columns).noneMatch(Objects::isNull)){
                    break;
                }
            }
            if(Arrays.stream(columns).anyMatch(Objects::isNull)){
                String columnNames = getColumnNamesWithAbsentMetadata(metaData, columnCount, columns);
                throw new IllegalArgumentException(
                        "Insufficient information in query to find type of columns: " + columnNames);
            }
        }

        String[] columnNames = new String[columnCount];
        String[] columnDefinition = new String[columnCount];
        for(int idx=1; idx<=columnCount; idx++){
            String columnName = metaData.getColumnName(idx);
            columnNames[idx-1] = columnName(columnName);
            int columnType = metaData.getColumnType(idx);
            if(columnType == Types.ARRAY){
                columnDefinition[idx-1] =columnDefinition ( columnName,
                        arrayColumnType(JAVA_TO_PG_MAPPING.getOrDefault(columns[idx-1],"?"))) ;
            } else
            if(columnType == Types.OTHER){
                String pgType=null;
                if(JAVA_TO_PG_MAPPING.containsKey(columns[idx-1])){
                    pgType = JAVA_TO_PG_MAPPING.get(columns[idx - 1]);
                }
                if(columns[idx-1]!=null && pgType==null &&
                        "org.postgresql.util.PGobject".equals(columns[idx-1].getName())){
                    pgType = pgObjectType[idx-1];
                }
                if(pgType==null){
                    pgType = "?";
                }
                columnDefinition[idx-1] = columnDefinition (columnName, pgType);

            } else {
                columnDefinition[idx-1] = columnDefinition ( columnName,
                        TYPE_MAPPING.getOrDefault(columnType, "?")) ;
            }
        }
        return new String[]{
            String.join(", ", columnDefinition),
            String.join(", ", columnNames)};
    }

    private static String getPgObjectSubtype(Class<?> column, Object object) {
        try {
            Method getType = column.getMethod("getType");
            return (String) getType.invoke(object);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    private static String getColumnNamesWithAbsentMetadata(ResultSetMetaData metaData,
                                                           int columnCount, Class<?>[] columns) {
        return IntStream.range(1, columnCount + 1).filter(value -> columns[value - 1] == null).
                mapToObj(value -> getColumnName(metaData, value)).collect(Collectors.joining(","));
    }

    private static boolean isInspectComplexTypes(ResultSetMetaData metaData, int columnCount) {
        return IntStream.range(1, columnCount + 1).map(value -> getColumnType(metaData, value)).
                anyMatch(value -> value == Types.ARRAY || value == Types.OTHER);
    }

    private static String getColumnName(ResultSetMetaData metaData, int value){
        try {
            return metaData.getColumnName(value);
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    private static int getColumnType(ResultSetMetaData metaData, int value){
        try {
            return metaData.getColumnType(value);
        } catch (SQLException throwables) {
            throw new RuntimeException(throwables);
        }
    }

    public static String columnDefinition(String name, String type){
        return String.format("\"%s\" %s", name, type);
    }

    public static String arrayColumnType(String type){
        return String.format("%s[]", type);
    }

    public static String columnName(String name){
        return String.format("\"%s\"",name);
    }
}
