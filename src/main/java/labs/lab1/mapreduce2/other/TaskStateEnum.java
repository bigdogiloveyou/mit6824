package labs.lab1.mapreduce2.other;

/**
 * @author xushu
 * @create 8/10/21 9:06 PM
 * @desc 任务类型，即所处阶段
 */
public enum TaskStateEnum {
    Map(0),
    Reduce(1),
    Exit(2),
    Wait(3);


    private int type;

    TaskStateEnum(int type) {
        this.type = type;
    }
}
