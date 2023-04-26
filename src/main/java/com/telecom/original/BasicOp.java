package com.telecom.original;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

public interface BasicOp {
    public SparkSession env_set(final String appname, final String master_set, final String loglevel);
    public SparkSession env_set_hive(final String appname, final String master_set, final String loglevel);
    public Dataset<Row> get_data_from_excel(final String path, SparkSession spark);
    public Dataset<Row> get_data_from_excel(final String path, SparkSession sparkfinal, StructType structType);
    public Dataset<Row> get_data_from_jdbc(SparkSession spark, final String url, final String driver, final String user,
                                           final String password, final String dbtable);
    public Dataset<Row> get_data_from_hive(SparkSession spark, final String databaseName, final String tableName);
    public long get_data_total_row_num(Dataset<Row> data);
    public int get_data_total_col_num(Dataset<Row> data);

    public void result_save(Dataset<Row> data, final String path, final String file_style);
}
