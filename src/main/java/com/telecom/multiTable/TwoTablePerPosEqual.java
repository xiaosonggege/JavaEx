package com.telecom.multiTable;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import com.telecom.original.*;
import java.util.*;
import lombok.Setter;
import lombok.Getter;

/**
 Two tables per position check
 check_col: List<List<String>>
 */
public class TwoTablePerPosEqual extends CheckBasic{

    private String data_export_path2 = null;
    @Getter @Setter private String dbtable2 = null;
    @Getter @Setter private String tableName2 = null;

    private String join_col_name1;
    private String join_col_name2;
    private String join_mode;
    private List<ArrayList<String>> check_col;

    /*
    excel,cvs,txt,json
    **/
    public TwoTablePerPosEqual(String appname, String master_set, String loglevel, String data_export_path,
                               String data_save_path, String data_save_style, String data_export_path2,
                               String join_col_name1, String join_col_name2, String join_mode, List<ArrayList<String>> check_col) {
        super(appname, master_set, loglevel, data_export_path, data_save_path, data_save_style);
        this.data_export_path2 = data_export_path2;
        this.join_col_name1 = join_col_name1;
        this.join_col_name2 = join_col_name2;
        this.join_mode = join_mode;
        this.check_col = check_col;
    }
    /*
    jdbc
    **/
    public TwoTablePerPosEqual(String appname, String master_set, String loglevel, String url, String driver, String user,
                               String password, String dbtable1, String dbtable2,
                               String join_col_name1, String join_col_name2, String join_mode, List<ArrayList<String>> check_col) {
        super(appname, master_set, loglevel, url, driver, user, password, dbtable1);
        this.dbtable2 = dbtable2;
        this.join_col_name1 = join_col_name1;
        this.join_col_name2 = join_col_name2;
        this.join_mode = join_mode;
        this.check_col = check_col;
    }

    /*
    hive
    **/
    public TwoTablePerPosEqual(String appname, String master_set, String loglevel, String databaseName, String tableName1,
                               String tableName2, String join_col_name1, String join_col_name2, String join_mode, List<ArrayList<String>> check_col) {
        super(appname, master_set, loglevel, databaseName, tableName1);
        this.tableName2 = tableName2;
        this.join_col_name1 = join_col_name1;
        this.join_col_name2 = join_col_name2;
        this.join_mode = join_mode;
        this.check_col = check_col;
    }

    public String get_data_export_path2(){return this.data_export_path2;}
    public void set_data_export_path2(final String data_export_path2){this.data_export_path2 = data_export_path2;}
    public String get_join_col_name1(){return this.join_col_name1;}
    public void set_join_col_name1(final String join_col_name1){this.join_col_name1 = join_col_name1;}
    public String get_join_col_name2(){return this.join_col_name2;}
    public void set_join_col_name2(final String join_col_name2){this.join_col_name2 = join_col_name2;}
    public String get_join_mode(){return this.join_mode;}
    public void set_join_mode(final String join_mode){this.join_mode = join_mode;}
    public List<ArrayList<String>> get_check_col(){return this.check_col;}
    public void set_check_col(final List<ArrayList<String>> check_col){this.check_col = check_col;}

    public void call(StructType structType1, StructType structType2, final String data_source, final boolean is_testing, List<Dataset<Row>> dflist) throws Exception{
        SparkSession spark = null;
        Dataset<Row> dataframe1 = null, dataframe2 = null;
        System.out.println("data_source: " + data_source);
        try{
            if (is_testing){
                spark = this.env_set(this.appname, this.master_set, this.loglevel);
                dataframe1 = dflist.get(0);
                dataframe2 = dflist.get(1);
            }
            else if (data_source.equals("excel")){
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
            //数据分析/////////
            spark.udf().register("checkEqual", new NullCheck(), DataTypes.BooleanType);
            Dataset<Row> joint_dataframe = dataframe1
                    .join(dataframe2, dataframe1.col("DR1_"+this.join_col_name1).equalTo(dataframe2.col("DR2_"+this.join_col_name2)), this.join_mode);
            try{
//            System.out.println("check_col size= " + this.check_col.size());
                if (this.check_col.size() == 0) throw new Exception();
//                spark.udf().register("checkEqual", new NullCheck(), DataTypes.BooleanType);
                //用单列结果初始化待输出的表
                Dataset<Row> bool_dataframe = joint_dataframe.select(
                        functions.col("DR1_"+this.join_col_name1).alias(this.join_col_name1),
                        functions.callUDF("checkEqual", functions.col("DR1_"+this.check_col.get(0).get(0)),
                                functions.col("DR2_"+this.check_col.get(0).get(1))).alias(this.check_col.get(0).get(0)+"_diff")
                );
                //再与其它待查询列进行拼接
                for (int i=1; i<this.check_col.size(); ++i){
                    Dataset<Row> temp = joint_dataframe.select(
                            functions.col("DR1_"+this.join_col_name1).alias("temp_"+this.join_col_name1),
                            functions.callUDF("checkEqual", functions.col("DR1_"+this.check_col.get(i).get(0)),
                                            functions.col("DR2_"+this.check_col.get(i).get(1)))
                                    .alias(this.check_col.get(i).get(0)+"_diff")
                    );
//                temp.show();
                    bool_dataframe = bool_dataframe
                            .join(temp, bool_dataframe.col(this.join_col_name1).equalTo(temp.col("temp_"+this.join_col_name1)), this.join_mode)
                            .drop("temp_"+this.join_col_name1);
//                bool_dataframe.show();
                }
                bool_dataframe.show(100, false);
                this.result_save(bool_dataframe, this.data_save_path, this.data_save_style);
            } catch (Exception e){
                System.out.println("check_col can't be empty!");
            } finally {
                spark.stop();
            }
        }

//        SparkSession spark = this.env_set(this.appname, this.master_set, this.loglevel);
//        //TODO: 实现可以选择从哪个数据库读入的功能
//        Dataset<Row> dataframe1 = null;
//        Dataset<Row> dataframe2 = null;
//        if (is_testing){
//            dataframe1 = dflist.get(0);
//            dataframe2 = dflist.get(1);
//        }
//        else{
//            dataframe1 = structType1 != null?
//                    this.get_data_from_excel(this.data_export_path, spark, structType1): this.get_data_from_excel(this.data_export_path, spark);
//
//            dataframe2 = structType2 != null?
//                    this.get_data_from_excel(this.data_export_path2, spark, structType2): this.get_data_from_excel(this.data_export_path2, spark);
//        }

    }
}

class NullCheck implements UDF2<String, String, Boolean> {
    @Override
    public Boolean call(String col1, String col2) throws Exception {
        if (col1 == null) return col2 == null;
        else if (col2 == null) return false; //col1!=null
        else return col1.equals(col2);
    }
}
