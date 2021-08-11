package labs.lab1.mapreduce2.other;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

/**
 * @author xushu
 */
@Getter
@Setter
@Builder
public class Request<T> implements Serializable {

    private T obj;

    private MessageType messageType;


}
