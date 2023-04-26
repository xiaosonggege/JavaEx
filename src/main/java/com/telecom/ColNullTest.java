package com.telecom;
import com.telecom.singleTable.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.*;
import com.telecom.requirementConfig.YamlReader;

public class ColNullTest {
    public static void main( String[] args ) {
        //判断字段字符串长度是否满足要求
        //导入yaml配置信息
        final String file_path = "/Users/songyunlong/Desktop/DataCheck/src/main/java/com/telecom/requirementConfig/col_null_test_config.yaml";
        Map<String, Object> conf = YamlReader.getInstance(file_path).getConf();
        System.out.println(conf);
        final String appname = YamlReader.getInstance(file_path).getString("appname");
        final String master_set = YamlReader.getInstance(file_path).getString("master_set");
        final String loglevel = YamlReader.getInstance(file_path).getString("loglevel");
        final String data_export_path = YamlReader.getInstance(file_path).getString("data_export_path");
        final String data_save_path = YamlReader.getInstance(file_path).getString("data_save_path");
        final String data_save_style = YamlReader.getInstance(file_path).getString("data_save_style");
        final Boolean need_structType = YamlReader.getInstance(file_path).getBoolean("need_structType", false);
        final String data_source = YamlReader.getInstance(file_path).getString("data_source");
        final String need_check_col_name = YamlReader.getInstance(file_path).getString("need_check_col_name");
        CheckColumnNullType check_col_null_type = new CheckColumnNullType(
                appname,
                master_set,
                loglevel,
                data_export_path,
                data_save_path,
                data_save_style
        );
        StructType structType = null;
        if (need_structType){
            final List<String> col_name = YamlReader.getInstance(file_path).getList("col_name");
            List<StructField> structFields = new ArrayList<>();
            for (String i: col_name) structFields.add(DataTypes.createStructField(i, DataTypes.StringType, true));

            structType = DataTypes.createStructType(structFields);
        }
        try{

            check_col_null_type.call(structType, data_source, need_check_col_name);
        } catch (Exception e){
            e.printStackTrace();
        }

    }
}
