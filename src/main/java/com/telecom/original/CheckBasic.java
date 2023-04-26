package com.telecom.original;
import com.telecom.original.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import lombok.Getter;
import lombok.Setter;

public abstract class CheckBasic implements BasicOp {
    protected String appname;
    protected String master_set;
    protected String loglevel;

    //from csv
    protected String data_export_path = null;
    protected String data_save_path = null;
    protected String data_save_style = null;

    //from jdbc
    protected String url = null;
    protected String driver = null;
    protected String user = null;
    protected String password = null;
    protected String dbtable = null;

    //from hive
    @Getter @Setter protected String databaseName = null;
    @Getter @Setter protected String tableName = null;

    /*
    excel,csv,json,txt
    **/
    protected CheckBasic(String appname, String master_set, String loglevel, String data_export_path,
               String data_save_path, String data_save_style){
        this.appname = appname;
        this.master_set = master_set;
        this.loglevel = loglevel;
        this.data_export_path = data_export_path;
        this.data_save_path = data_save_path;
        this.data_save_style = data_save_style;
    }

    /*
    hive
    **/
    protected CheckBasic(String appname, String master_set, String loglevel, String databaseName, String tableName){
        this.appname = appname;
        this.master_set = master_set;
        this.loglevel = loglevel;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    /*
    jdbc
    **/
    protected CheckBasic(String appname, String master_set, String loglevel, String url, String driver, String user,
                         String password, String dbtable) {
        this.appname = appname;
        this.master_set = master_set;
        this.loglevel = loglevel;
        this.url = url;
        this.driver = driver;
        this.user = user;
        this.password = password;
        this.dbtable = dbtable;
    }

    public String get_appname(){return this.appname;}
    public String get_master_set(){return this.master_set;}
    public String get_loglevel(){return this.loglevel;}
    public String get_data_export_path(){return this.data_export_path;}
    public String get_data_save_path(){return this.data_save_path;}
    public String get_data_save_style(){return this.data_save_style;}
    public String get_url(){return this.url;}
    public String get_driver(){return this.driver;}
    public String get_user(){return this.user;}
    public String get_password(){return this.password;}
    public String get_dbtable(){return this.dbtable;}

    public void set_appname(String appname){this.appname = appname;}
    public void set_master_set(String master_set){this.master_set = master_set;}
    public void set_loglevel(String loglevel){this.loglevel = loglevel;}
    public void set_data_export_path(String data_export_path){this.data_export_path = data_export_path;}
    public void set_data_save_path(String data_save_path){this.data_save_path = data_save_path;}
    public void set_data_save_style(String data_save_style){this.data_save_style = data_save_style;}
    public void set_url(String url){this.url = url;}
    public void set_driver(String driver){this.driver = driver;}
    public void set_user(String user){this.user = user;}
    public void set_password(String password){this.password = password;}
    public void set_dbtable(String dbtable){this.dbtable = dbtable;}

    @Override
    public SparkSession env_set(String appname, String master_set, String loglevel) {
        SparkSession spark = SparkSession.builder().appName(appname)
                .master(master_set)
                .getOrCreate();
        spark.sparkContext().setLogLevel(loglevel);
        return spark;
    }

    @Override
    /*
    待测试
    **/
    public SparkSession env_set_hive(String appname, String master_set, String loglevel){
        SparkSession spark = SparkSession.builder().appName(appname)
                .master(master_set)
                .enableHiveSupport()
                .getOrCreate();
        spark.sparkContext().setLogLevel(loglevel);
        return spark;
    }

    @Override
    public Dataset<Row> get_data_from_excel(String path, SparkSession spark) {
        Dataset<Row> result = spark.read().format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("treatEmptyValuesAsNulls", "true")
                // 自动推断schema
                .option("inferSchema", "true")
                .load(path);

        return result;
    }
    //TODO: 实现从其他数据库读入的功能

    @Override
    public Dataset<Row> get_data_from_excel(String path, SparkSession spark, StructType structType) {
        Dataset<Row> result = spark.read().format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("treatEmptyValuesAsNulls", "true")
                .schema(structType)
                .load(path);
        return result;
    }

    @Override
    public Dataset<Row> get_data_from_jdbc(SparkSession spark, String url, String driver, String user, String password, String dbtable){
        Dataset<Row> result = spark.read().format("jdbc")
                .option("url", url)
                .option("driver", driver)
                .option("user", user)
                .option("password", password)
                .option("dbtable", dbtable)
                .load();
        return result;
    }

    @Override
    public Dataset<Row> get_data_from_hive(SparkSession spark, String databaseName, String tableName){
        Dataset<Row> result = spark.sql("select * from " + databaseName + "." + tableName);
        return result;
    }

    @Override
    public long get_data_total_row_num(Dataset<Row> data) {
        return data.count();
    }

    @Override
    public int get_data_total_col_num(Dataset<Row> data) {
        return data.columns().length;
    }

    @Override
    public void result_save(Dataset<Row> data, String path, final String file_style){
        /*
        默认保存为csv格式
        */
        data.repartition(1).write().option("header", "true").mode("overwrite").format(file_style).save(path);
    }
}
