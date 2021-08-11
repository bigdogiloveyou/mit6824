package labs.lab1.mapreduce2.other;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.List;

/**
 * @author xushu
 * @create 8/10/21 9:08 PM
 * @desc
 */
@Getter
@Setter
@Builder
public class Task implements Serializable {

    /**
     * 输入文件
     */
    private String input;

    /**
     * task 状态
     */
    private TaskStateEnum taskState;

    /**
     * 切分的  redcue 数量
     */
    private int nReduce;

    /**
     * 任务 id
     */
    private int TaskNumber;

    /**
     * 中间结果文件，即被切分的 reduce 文件
     */
    private List<String> intermediates;

    /**
     * 最后的输出结果
     */
    private String output;

}
