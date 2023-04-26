package com.telecom.requirementConfig;
import com.telecom.requirementConfig.*;
import org.yaml.snakeyaml.Yaml;

import java.util.*;
public class UseConfig {
    public static void main(String[] args){
        final String file_path = "/Users/songyunlong/Desktop/DataCheck/src/main/java/com/telecom/requirementConfig/example_test_config.yaml";
//        Map<String, Object> conf = YamlReader.getInstance(file_path).getConf();
//        System.out.println(conf);
//        String parma1 = YamlReader.getInstance(file_path).getString("data_save_path");
//        System.out.println(parma1);
//        List<String> col_name = YamlReader.getInstance(file_path).getList("col_name");
//        for (String i: col_name) System.out.println(i);
//        System.out.println();
//        List<ArrayList<String>> need_check_col = YamlReader.getInstance(file_path).get2dimList("need_check_col");
//        for (ArrayList<String> l: need_check_col){
//            System.out.println(l.get(0) + " " + l.get(1));
//        }
//        int len = YamlReader.getInstance(file_path).getInteger("len", false);
//        System.out.println(len + ": " + (len+10));
//
//        double len1 = YamlReader.getInstance(file_path).getDouble("len1", false);
//        System.out.println(len1 + ": " + (len1+10));
//
//        boolean len2 = YamlReader.getInstance(file_path).getBoolean("len2", false);
//        System.out.println(len2 + ": " + (!len2));

        ArrayList<String> al = new ArrayList<>(Arrays.asList("song", "yun"));
        String a = "excel";
        System.out.println(a == "excel");
        String b = "excel";
        System.out.println(a == b);
        System.out.println(a.equals(b));
    }
}
