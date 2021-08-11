package labs.lab1.mapreduce2.other;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * @author xushu
 * @create 8/10/21 9:54 PM
 * @desc
 */
@Setter
@Getter
@Builder
public class KeyValue {

    private String key;
    private String value;


}
