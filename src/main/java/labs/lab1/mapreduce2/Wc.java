package labs.lab1.mapreduce2;

import labs.lab1.mapreduce2.other.KeyValue;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.*;

/**
 * @author xushu
 * @create 8/9/21 9:46 PM
 * @desc
 */
public class Wc {

    public static List<KeyValue> mapFunction(String inputfileName) throws Exception {
        File file = new File(inputfileName);
        if(!file.exists()){
            throw new Exception("文件不存在");
        }

        List<KeyValue> list = new ArrayList<>();
        List<String> lines = FileUtils.readLines(file, "UTF-8");
        for (String line : lines) {
            StringTokenizer tokenizer = new StringTokenizer(line, ",!' '.;()*--[]:\"\"?");
            while (tokenizer.hasMoreTokens()){
                list.add(KeyValue.builder()
                        .key(tokenizer.nextToken())
                        .value("1")
                        .build());
            }
        }

        return list;
    }


    public static Map<String, Integer> reduceFunction(String mapOutputFileName) throws Exception {
        File file = new File(mapOutputFileName);
        if(!file.exists()){
            throw new Exception("文件不存在");
        }

        Map<String, Integer> map = new HashMap<>();
        List<String> lines = FileUtils.readLines(file, "UTF-8");
        for (String line : lines) {
            String[] str = line.split("\\s+");
            String key = str[0];
            String values = str[1];
            map.put(key, map.getOrDefault(key, 0) + values.split(",").length);
        }

        return map;
    }
}
