package labs.lab1.mapreduce2.other;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 *
 * @author xushu
 */
@Getter
@Setter
@Builder
public class Response<T> implements Serializable {


    private T result;
}
