package com.telecom.singleTable;
import com.telecom.original.CheckBasic;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.*;
import org.apache.spark.sql.*;
import java.util.*;

public class CheckColumnType extends CheckBasic{
    /*
    excel,csv,txt,json
    **/
    public CheckColumnType(String appname, String master_set, String loglevel, String data_export_path,
                                   String data_save_path, String data_save_style){
        super(appname, master_set, loglevel, data_export_path, data_save_path, data_save_style);
    }
    /*
    jdbc
    **/
    public CheckColumnType(String appname, String master_set, String loglevel, String url, String driver,
                           String user, String password, String dbtable){
        super(appname, master_set, loglevel, url, driver, user, password, dbtable);
    }
    /*
    hive
    **/
    public CheckColumnType(String appname, String master_set, String loglevel, String databaseName, String tableName){
        super(appname, master_set, loglevel, databaseName, tableName);
    }

    public void call(StructType structType, final String data_source, final String need_check_col_name) throws Exception{
        /*
        从col中筛选出为非数值类型的row都有哪些
        @data_source: csv, txt, excel, json, jdbc, hive
        */
        SparkSession spark = null;
        Dataset<Row> dataframe = null;
        System.out.println("data_source: " + data_source);
        try{
            if (data_source.equals("excel")){
                spark = this.env_set(this.appname, this.master_set, this.loglevel);
                dataframe = structType != null?
                        this.get_data_from_excel(this.data_export_path, spark, structType): this.get_data_from_excel(this.data_export_path, spark);
            }
            else if (data_source.equals("jdbc")){
                spark = this.env_set(this.appname, this.master_set, this.loglevel);
                dataframe = this.get_data_from_jdbc(spark, this.url, this.driver, this.user, this.password, this.dbtable);
            }
            else if (data_source.equals("hive")){
                spark = this.env_set_hive(this.appname, this.master_set, this.loglevel);
                dataframe = this.get_data_from_hive(spark, this.databaseName, this.tableName);
            }
            //TODO: 实现可以选择从哪个数据库读入的功能(csv,txt,json)
            else{
                throw new Exception();
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            //先检查待检测字段是否存在空值，如果有，把空值行去掉，如果可以，需要先统计出空值数量
            Dataset<Row> temp = dataframe.filter(functions.col(need_check_col_name).isNotNull());
            spark.udf().register("checkType", (String col) -> col.matches("^\\d+?\\.??\\d+?$"), DataTypes.BooleanType);
            Dataset<Row> result = temp
                    .filter(functions.callUDF("checkType", functions.col(need_check_col_name)).equalTo(false));
            result.show(false);
            this.result_save(result, this.data_save_path, this.data_save_style);
            System.out.println("not satisfied number: " + result.count());
            System.out.println("null total number: " + (dataframe.count()-temp.count()));
            System.out.println("total number: " + dataframe.count());
            spark.stop();
        }
    }
}
