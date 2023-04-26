package com.telecom;
import com.telecom.multiTable.*;
import com.telecom.requirementConfig.YamlReader;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TwoTablePerPosTest {
    public static void main( String[] args ) throws Exception {
        // 双表稽查，先检查两个表中id是否都相互包含，如果满足，再检查对应id的所有对应字段是否取值相同，根据需求找出不同取值的
        //导入yaml配置信息
        final String file_path = "/Users/songyunlong/Desktop/DataCheck/src/main/java/com/telecom/requirementConfig/two_tables_per_pos_test_config.yaml";
        Map<String, Object> conf = YamlReader.getInstance(file_path).getConf();
        System.out.println(conf);
        final String appname = YamlReader.getInstance(file_path).getString("appname");
        final String master_set = YamlReader.getInstance(file_path).getString("master_set");
        final String loglevel = YamlReader.getInstance(file_path).getString("loglevel");
        final Boolean need_structType = YamlReader.getInstance(file_path).getBoolean("need_structType", false);
        final String data_export_path = YamlReader.getInstance(file_path).getString("data_export_path");
        final String data_export_path2 = YamlReader.getInstance(file_path).getString("data_export_path2");
        final String data_save_path = YamlReader.getInstance(file_path).getString("data_save_path");
        final String data_save_style = YamlReader.getInstance(file_path).getString("data_save_style");
        final String join_col_name1 = YamlReader.getInstance(file_path).getString("join_col_name1");
        final String join_col_name2 = YamlReader.getInstance(file_path).getString("join_col_name2");
        final String join_mode = YamlReader.getInstance(file_path).getString("join_mode");
        final String data_source = YamlReader.getInstance(file_path).getString("data_source");

        //示例数据//////////////////
        SparkSession spark = SparkSession.builder().appName("JavaSparkDemo")
                .master("local[*]")
                .getOrCreate();
        spark.sparkContext().setLogLevel("Warn");
        // 双表稽查，先检查两个表中id是否都相互包含，如果满足，再检查对应id的所有对应字段是否取值相同，根据需求找出不同取值的
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("code", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("parent_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("area_level_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("ppm_code", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("spec_id", DataTypes.StringType, true));
        structFields.add(DataTypes.createStructField("is_valid", DataTypes.StringType, true));
        StructType structType = DataTypes.createStructType(structFields);

        Dataset<Row> excelDR1 = spark.read().format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("treatEmptyValuesAsNulls", "true")
                // 自动推断schema
//                .option("inferSchema", "true")
                .schema(structType)
                .load("/Users/songyunlong/Desktop/company/数据治理/数据质量管理/数据/电信管理域主数据_temp2.xlsx");
//        excelDR1.show(20, false);
//        excelDR1.printSchema();

        Dataset<Row> excelDR2 = spark.read().format("com.crealytics.spark.excel")
                .option("header", "true")
                .option("treatEmptyValuesAsNulls", "true")
                // 自动推断schema
//                .option("inferSchema", "true")
                .schema(structType)
                .load("/Users/songyunlong/Desktop/company/数据治理/数据质量管理/数据/电信管理域主数据_temp22.xlsx");
//        excelDR2.show(20, false);
//        excelDR2.printSchema();

//        Dataset<Row> temp1 = excelDR1
//                .join(excelDR2, excelDR1.col("id").equalTo(excelDR2.col("id")), "left")
//                .select(excelDR1.col("id").alias("DR1_id"), excelDR2.col("id").alias("DR2_id"))
//                .where(functions.col("DR2_id")
//                .isNull());
//        temp1.show(100, false);
//        long temp2_has_temp22_not_has = temp1.count();
//        System.out.println("temp2中有而temp22中没有的数量为: " + temp2_has_temp22_not_has);
//
//        Dataset<Row> temp2 = excelDR2
//                .join(excelDR1, excelDR2.col("id").equalTo(excelDR1.col("id")), "left")
//                .select(excelDR2.col("id").alias("DR2_id"), excelDR1.col("id").alias("DR1_id"))
//                .where(functions.col("DR1_id")
//                        .isNull());
//        temp2.show(100, false);
//        long temp22_has_temp2_not_has = temp2.count();
//        System.out.println("temp22中有而temp2中没有的数量为: " + temp22_has_temp2_not_has);

//         双表对应idx、对应col进行逐个字段审核是否一致
//        构造两个idx和col都相同的表进行实验
        Dataset<Row> df = excelDR1
                .join(excelDR2, excelDR1.col("id").equalTo(excelDR2.col("id")), "inner")
                .select(excelDR1.col("id").alias("DR1_id"),
                        excelDR1.col("code").alias("DR1_code"),
                        excelDR1.col("name").alias("DR1_name"),
                        excelDR1.col("parent_id").alias("DR1_parent_id"),
                        excelDR1.col("area_level_id").alias("DR1_area_level_id"),
                        excelDR1.col("ppm_code").alias("DR1_ppm_code"),
                        excelDR1.col("spec_id").alias("DR1_spec_id"),
                        excelDR1.col("is_valid").alias("DR1_is_valid"),
                        excelDR2.col("id").alias("DR2_id"),
                        excelDR2.col("code").alias("DR2_code"),
                        excelDR2.col("name").alias("DR2_name"),
                        excelDR2.col("parent_id").alias("DR2_parent_id"),
                        excelDR2.col("area_level_id").alias("DR2_area_level_id"),
                        excelDR2.col("ppm_code").alias("DR2_ppm_code"),
                        excelDR2.col("spec_id").alias("DR2_spec_id"),
                        excelDR2.col("is_valid").alias("DR2_is_valid")
                        );
        Dataset<Row> df1 = df.select(
                df.col("DR1_id"),
                df.col("DR1_code"),
                df.col("DR1_name"),
                df.col("DR1_parent_id"),
                df.col("DR1_area_level_id"),
                df.col("DR1_ppm_code"),
                df.col("DR1_spec_id"),
                df.col("DR1_is_valid")
                );
        Dataset<Row> df2 = df.select(
                df.col("DR2_id"),
                df.col("DR2_code"),
                df.col("DR2_name"),
                df.col("DR2_parent_id"),
                df.col("DR2_area_level_id"),
                df.col("DR2_ppm_code"),
                df.col("DR2_spec_id"),
                df.col("DR2_is_valid")
                );
//        df1.show(100, false);
//        df2.show(100, false);
        ////////示例数据over//////////

        List<ArrayList<String>> need_check_col = YamlReader.getInstance(file_path).get2dimList("need_check_col");
//        List<ArrayList<String>> need_check_col = new ArrayList<>(Arrays.asList(
//                new ArrayList<>(Arrays.asList("id", "id")),
//                new ArrayList<>(Arrays.asList("code", "code")),
//                new ArrayList<>(Arrays.asList("name", "name")),
//                new ArrayList<>(Arrays.asList("parent_id", "parent_id")),
//                new ArrayList<>(Arrays.asList("area_level_id", "area_level_id")),
//                new ArrayList<>(Arrays.asList("ppm_code", "ppm_code")),
//                new ArrayList<>(Arrays.asList("spec_id", "spec_id")),
//                new ArrayList<>(Arrays.asList("is_valid", "is_valid"))
//        ));

        TwoTablePerPosEqual ttppe = new TwoTablePerPosEqual(
                appname,
                master_set,
                loglevel,
                data_export_path,
                data_save_path,
                data_save_style,
                data_export_path2,
                join_col_name1,
                join_col_name2,
                join_mode,
                need_check_col
        );
        ttppe.call(null, null, data_source, true, Arrays.asList(df1, df2));
    }
}
