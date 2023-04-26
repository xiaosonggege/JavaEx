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


public class CheckColumnStringLength extends CheckBasic {
    protected int len;
    /*
    excel,csv,txt,json
    **/
    public CheckColumnStringLength(String appname, String master_set, String loglevel, String data_export_path,
                            String data_save_path, String data_save_style, int len){
        super(appname, master_set, loglevel, data_export_path, data_save_path, data_save_style);
        this.len = len;
    }
    /*
    jdbc
    **/
    public CheckColumnStringLength(String appname, String master_set, String loglevel, String url, String driver, String user,
                                   String password, String dbtable, int len){
        super(appname, master_set, loglevel, url, driver, user, password, dbtable);
        this.len = len;
    }
    /*
    hive
    **/
    public CheckColumnStringLength(String appname, String master_set, String loglevel, String databaseName,
                                   String tableName, int len){
        super(appname, master_set, loglevel, databaseName, tableName);
        this.len = len;
    }

    public int get_len(){return this.len;}
    public void set_len(final int len){this.len = len;}

    public void call(StructType structType, final String data_source, final String need_check_col_name) throws Exception{
        /*
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
            //数据分析
            spark.udf().register("checklength", new CLUDF(this.len), DataTypes.BooleanType);
            Dataset<Row> result = dataframe.filter(
                    functions.callUDF("checklength", functions.col(need_check_col_name)).equalTo(false));
            result.show(false);
            this.result_save(result, this.data_save_path, this.data_save_style);
            System.out.println("not satisfied number: " + result.count());
            spark.stop();
        }
    }
}

class CLUDF implements UDF1<String, Boolean> {
    private Integer len;
    public CLUDF(Integer len){
        this.len = len;
    }
    @Override
    public Boolean call(String col) throws Exception{
        return col.length() == this.len;
    }
}