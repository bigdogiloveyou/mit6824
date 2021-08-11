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

    public static List<KeyValue> mapFunction(List<String> lines)  {
        List<KeyValue> list = new ArrayList<>();
        for (String line : lines) {
            StringTokenizer tokenizer = new StringTokenizer(line, ",!' '.;()*--[]:\"\"?#$&!@");
            while (tokenizer.hasMoreTokens()){
                list.add(KeyValue.builder()
                        .key(tokenizer.nextToken())
                        .value("1")
                        .build());
            }
        }

        return list;
    }


    public static int reduceFunction(List<KeyValue> keyValues)  {
        return keyValues.size();
    }
}
