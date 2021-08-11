package labs.lab1.mapreduce2.other;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

/**
 * @author xushu
 * @create 8/10/21 9:13 PM
 * @desc 任务的元数据信息
 */
@Getter
@Setter
@Builder
public class TaskMetaInfo {

    /**
     * 任务的状态，空闲、流程中、完成
     */
    private TaskMetaInfoEnum taskMetaInfoEnum;

    /**
     * 任务分配时间
     */
    private Long startTime;

    /**
     * 对应的任务，为啥要这样，因为 task 被 taskQueue 移除后，taskMetaInfo 记录了 task，超时会重新放进去
     */
    private Task task;
}
