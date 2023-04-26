package com.telecom;
import com.telecom.multiTable.*;
import com.telecom.requirementConfig.YamlReader;
import com.telecom.singleTable.CheckColumnType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class TwoTablesTest {
    public static void main( String[] args ) {
        //判断差集
        //导入yaml配置信息
        final String file_path = "/Users/songyunlong/Desktop/DataCheck/src/main/java/com/telecom/requirementConfig/two_tables_test_config.yaml";
        Map<String, Object> conf = YamlReader.getInstance(file_path).getConf();
        System.out.println(conf);
        final String appname = YamlReader.getInstance(file_path).getString("appname");
        final String master_set = YamlReader.getInstance(file_path).getString("master_set");
        final String loglevel = YamlReader.getInstance(file_path).getString("loglevel");
        final String data_export_path = YamlReader.getInstance(file_path).getString("data_export_path");
        final String data_export_path2 = YamlReader.getInstance(file_path).getString("data_export_path2");
        final String data_save_path = YamlReader.getInstance(file_path).getString("data_save_path");
        final String data_save_style = YamlReader.getInstance(file_path).getString("data_save_style");
        final Boolean need_structType = YamlReader.getInstance(file_path).getBoolean("need_structType", false);
        final String join_col_name1 = YamlReader.getInstance(file_path).getString("join_col_name1");
        final String join_col_name2 = YamlReader.getInstance(file_path).getString("join_col_name2");
        final String join_mode = YamlReader.getInstance(file_path).getString("join_mode");
        final String data_source = YamlReader.getInstance(file_path).getString("data_source");

        DifferenceStatic2 differenceStatic2 = new DifferenceStatic2(
                appname,
                master_set,
                loglevel,
                data_export_path,
                data_save_path,
                data_save_style,
                data_export_path2,
                join_col_name1,
                join_col_name2,
                join_mode
        );
        StructType structType1 = null;
        StructType structType2 = null;
        if (need_structType){
            final List<String> col_name1 = YamlReader.getInstance(file_path).getList("col_name1");
            List<StructField> structFields1 = new ArrayList<>();
            for (String i: col_name1) structFields1.add(DataTypes.createStructField(i, DataTypes.StringType, true));
            structType1 = DataTypes.createStructType(structFields1);

            final List<String> col_name2 = YamlReader.getInstance(file_path).getList("col_name2");
            List<StructField> structFields2 = new ArrayList<>();
            for (String i: col_name2) structFields2.add(DataTypes.createStructField(i, DataTypes.StringType, true));

            structType2 = DataTypes.createStructType(structFields2);
        }

        try{
            differenceStatic2.call(structType1, structType2, data_source, true, true);
        } catch (Exception e){
            e.printStackTrace();
        }

    }
}
