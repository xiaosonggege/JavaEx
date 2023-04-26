package com.telecom.multiTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import com.telecom.original.*;
import lombok.Getter;
import lombok.Setter;
/**
Two tables difference check
 */
public class DifferenceStatic2 extends CheckBasic{
    private String data_export_path2 = null;
    private String join_col_name1;
    private String join_col_name2;
    private String join_mode;
    @Getter @Setter private String dbtable2 = null;
    @Getter @Setter private String tableName2 = null;
    /*
    excel,csv,txt,json
    **/
    public DifferenceStatic2(String appname, String master_set, String loglevel, String data_export_path, String data_save_path,
                                String data_save_style, String data_export_path2, String join_col_name1, String join_col_name2,
                                String join_mode) {
        super(appname, master_set, loglevel, data_export_path, data_save_path, data_save_style);
        this.data_export_path2 = data_export_path2;
        this.join_col_name1 = join_col_name1;
        this.join_col_name2 = join_col_name2;
        this.join_mode = join_mode;
    }
    /*
    jdbc
    **/
    public DifferenceStatic2(String appname, String master_set, String loglevel, String url, String driver, String user,
                             String password, String dbtable1, String dbtable2, String join_col_name1, String join_col_name2,
                             String join_mode) {
        super(appname, master_set, loglevel, url, driver, user, password, dbtable1);
        this.dbtable2 = dbtable2;
        this.join_col_name1 = join_col_name1;
        this.join_col_name2 = join_col_name2;
        this.join_mode = join_mode;
    }
    /*
    hive
    **/
    public DifferenceStatic2(String appname, String master_set, String loglevel, String databaseName, String tableName1,
                             String tableName2, String join_col_name1, String join_col_name2,
                             String join_mode) {
        super(appname, master_set, loglevel, databaseName, tableName1);
        this.tableName2 = tableName2;
        this.join_col_name1 = join_col_name1;
        this.join_col_name2 = join_col_name2;
        this.join_mode = join_mode;
    }


    public String get_data_export_path2(){return this.data_export_path2;}
    public void set_data_export_path2(final String data_export_path2){this.data_export_path2 = data_export_path2;}
    public String get_join_col_name1(){return this.join_col_name1;}
    public void set_join_col_name1(final String join_col_name1){this.join_col_name1 = join_col_name1;}
    public String get_join_col_name2(){return this.join_col_name2;}
    public void set_join_col_name2(final String join_col_name2){this.join_col_name2 = join_col_name2;}
    public String get_join_mode(){return this.join_mode;}
    public void set_join_mode(final String join_mode){this.join_mode = join_mode;}

    public void call(StructType structType1, StructType structType2, final String data_source, final Boolean need1diff2, final Boolean need2diff1) throws Exception{
        /*
        @data_source: csv, txt, excel, json, jdbc, hive
        */
        SparkSession spark = null;
        Dataset<Row> dataframe1 = null, dataframe2 = null;
        System.out.println("data_source: " + data_source);
        try{
            if (data_source.equals("excel")){
                spark = this.env_set(this.appname, this.master_set, this.loglevel);
                dataframe1 = structType1 != null?
                        this.get_data_from_excel(this.data_export_path, spark, structType1): this.get_data_from_excel(this.data_export_path, spark);
                dataframe2 = structType2 != null?
                        this.get_data_from_excel(this.data_export_path2, spark, structType2): this.get_data_from_excel(this.data_export_path2, spark);
            }
            else if (data_source.equals("jdbc")){
                spark = this.env_set(this.appname, this.master_set, this.loglevel);
                dataframe1 = this.get_data_from_jdbc(spark, this.url, this.driver, this.user, this.password, this.dbtable);
                dataframe2 = this.get_data_from_jdbc(spark, this.url, this.driver, this.user, this.password, this.dbtable2);
            }
            else if (data_source.equals("hive")){
                spark = this.env_set_hive(this.appname, this.master_set, this.loglevel);
                dataframe1 = this.get_data_from_hive(spark, this.databaseName, this.tableName);
                dataframe2 = this.get_data_from_hive(spark, this.databaseName, this.tableName2);
            }
            //TODO: 实现可以选择从哪个数据库读入的功能(csv,txt,json)
            else{
                throw new Exception();
            }
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            //数据分析
            try{
                if (!need1diff2 || !need2diff1){
                    System.out.println("need two tables!");
                    throw new Exception();
                }
                Dataset<Row> temp1 = dataframe1
                        .join(dataframe2, dataframe1.col(this.join_col_name1).equalTo(dataframe2.col(this.join_col_name2)), this.join_mode)
                        .select(dataframe1.col(this.join_col_name1).alias("DR1_"+this.join_col_name1),
                                dataframe2.col(this.join_col_name2).alias("DR2_"+this.join_col_name2))
                        .where(functions.col("DR2_"+this.join_col_name2)
                                .isNull());
                final long table1_has_table2_not_has = temp1.count();
                System.out.println("table1 has table2 doesn't have: " + table1_has_table2_not_has);
                this.result_save(temp1, this.data_save_path+"1", this.data_save_style);

                Dataset<Row> temp2 = dataframe2
                        .join(dataframe1, dataframe2.col(this.join_col_name2).equalTo(dataframe1.col(this.join_col_name1)), this.join_mode)
                        .select(dataframe2.col(this.join_col_name2).alias("DR2_"+this.join_col_name2),
                                dataframe1.col(this.join_col_name1).alias("DR1_"+this.join_col_name1))
                        .where(functions.col("DR1_"+this.join_col_name1)
                                .isNull());
                final long table2_has_table1_not_has = temp2.count();
                System.out.println("table2 has table1 doesn't have: " + table2_has_table1_not_has);
                this.result_save(temp2, this.data_save_path+"2", this.data_save_style);

            } catch (Exception e){
                e.printStackTrace();
            }
            finally {
                spark.stop();
            }
        }
    }
}
