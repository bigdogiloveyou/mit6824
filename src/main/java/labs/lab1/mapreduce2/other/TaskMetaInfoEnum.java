package labs.lab1.mapreduce2.other;

/**
 * @author xushu
 * @create 8/10/21 9:02 PM
 * @desc 协调者任务状态
 */
public enum TaskMetaInfoEnum {
    Idle(0),
    InProgress(1),
    Completed(2);


    private int status;

    TaskMetaInfoEnum(int status) {
        this.status = status;
    }
}
