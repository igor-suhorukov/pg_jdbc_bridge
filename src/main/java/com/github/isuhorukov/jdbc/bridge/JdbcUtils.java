package com.github.isuhorukov.jdbc.bridge;

import lombok.experimental.UtilityClass;

@UtilityClass
public class JdbcUtils {
    public static void initDriver(String driverClass) {
        if(driverClass !=null && !driverClass.isEmpty()){
            try {
                Class.forName(driverClass);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Driver "+ driverClass +" not found", e);
            }
        }
    }
}
