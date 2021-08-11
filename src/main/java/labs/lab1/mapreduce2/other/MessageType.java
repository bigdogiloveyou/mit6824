package labs.lab1.mapreduce2.other;


/**
 * woker 向 master 请求消息类型
 */
public enum MessageType {


    PULL_TASK("PULL_TASK"),
    TASK_COMPLETED("TASK_COMPLETED");

    private String type;

    MessageType(String type) {
        this.type = type;
    }
}