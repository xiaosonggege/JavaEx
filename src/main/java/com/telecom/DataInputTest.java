package com.telecom;
import com.telecom.singleTable.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class DataInputTest {
    public static void main(String[] args){
        CheckColumnType check_col_type = new CheckColumnType(
                "JavaSparkDemo",
                "local[*]",
                "Warn",
                "jdbc:mysql://localhost:3306/MyfirstDatabase?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=UTC",
                "com.mysql.cj.jdbc.Driver",
                "root",
                "xiaosonggege1025",
                "score"
        );
        SparkSession spark = check_col_type.env_set(
                check_col_type.get_appname(),
                check_col_type.get_master_set(),
                check_col_type.get_loglevel()
        );
        Dataset<Row> result = check_col_type.get_data_from_jdbc(
                spark,
                check_col_type.get_url(),
                check_col_type.get_driver(),
                check_col_type.get_user(),
                check_col_type.get_password(),
                check_col_type.get_dbtable()
        );
        result.show();
    }
}
