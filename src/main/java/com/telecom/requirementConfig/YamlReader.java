package com.telecom.requirementConfig;
import java.io.*;
import java.util.*;

import com.ctc.wstx.api.ReaderConfig;
import javafx.scene.layout.BorderImage;
import org.apache.spark.sql.sources.In;
import org.yaml.snakeyaml.Yaml;
import lombok.Getter;

public class YamlReader{
    @Getter private Map<String, Object> conf;
    private static YamlReader instance;

    private YamlReader(final String file_path){
        try{
            this.conf = (Map<String, Object>) new Yaml().load(new FileInputStream(new File(file_path)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static YamlReader getInstance(final String file_path){
        if (YamlReader.instance == null){
            synchronized (ReaderConfig.class){
                if (YamlReader.instance == null){
                    YamlReader.instance = new YamlReader(file_path);
                }
            }
        }
        return YamlReader.instance;
    }

    //读取String类型
    public String getString(String param){
        return String.valueOf(this.conf.get(param));
    }

    public String getString(String param, String defaultValue){
        if (null == this.conf.get(param)) {
            return defaultValue;
        }
        return String.valueOf(this.conf.get(param));
    }

    //读取Integer类型
    public Integer getInteger(String param, Boolean is_string){
        /*
        @is_string: yaml文件中Integer是否以String的形式写入
         */
        if (is_string) return Integer.valueOf((String) this.conf.get(param));
        else return (Integer) this.conf.get(param);
    }
    public Integer getInteger(String param, Boolean is_string, Integer defaultValue){
        if (null == this.conf.get(param)) {
            return defaultValue;
        }
        if (is_string) return Integer.valueOf((String) this.conf.get(param));
        else return (Integer) this.conf.get(param);
    }

    //读取Double类型
    public Double getDouble(String param, Boolean is_string){
        if (is_string) return Double.valueOf((String) this.conf.get(param));
        else return (Double) this.conf.get(param);
    }
    public Double getDouble(String param, Boolean is_string, Double defaultValue){
        if (null == this.conf.get(param)) {
            return defaultValue;
        }
        if (is_string) return Double.valueOf((String) this.conf.get(param));
        else return (Double) this.conf.get(param);
    }

    //读取Boolean类型
    public Boolean getBoolean(String param, Boolean is_string){
        if (is_string) return Boolean.valueOf((String) this.conf.get(param));
        else return (Boolean) this.conf.get(param);
    }
    public Boolean getBoolean(String param, Boolean is_string, Boolean defaultValue){
        if (null == this.conf.get(param)) {
            return defaultValue;
        }
        if (is_string) return Boolean.valueOf((String) this.conf.get(param));
        else return (Boolean) this.conf.get(param);
    }

    //读取ArrayList<E>类型，E为基础类型Integer，String，Double，Boolean
    public <E> ArrayList<E> getList(String param){
        return (ArrayList<E>) this.conf.get(param);
    }
    public <E> ArrayList<E> getList(String param, ArrayList<E> defaultValue){
        if (null == this.conf.get(param)){
            return defaultValue;
        }
        return (ArrayList<E>) this.conf.get(param);
    }

    //读取ArrayList<ArrayList<E>>类型，E为基础类型Integer，String，Double，Boolean
    public <E> ArrayList<ArrayList<E>> get2dimList(String param){
        ArrayList<ArrayList<E>> result = new ArrayList<>();
        for (ArrayList<E> l: (ArrayList<ArrayList<E>>) this.conf.get(param)){
            result.add((ArrayList<E>) l);
        }
        return result;
    }
    public <E> ArrayList<ArrayList<E>> get2dimList(String param, ArrayList<ArrayList<E>> defaultValue){
        if (this.conf.get(param) == null) return defaultValue;
        ArrayList<ArrayList<E>> result = new ArrayList<>();
        for (ArrayList<E> l: (ArrayList<ArrayList<E>>) this.conf.get(param)){
            result.add(l);
        }
        return result;
    }
    //todo: 碰到更复杂的类型需要补充

}
