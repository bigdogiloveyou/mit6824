package labs.lab1.mapreduce2;

import com.alipay.remoting.rpc.RpcClient;
import labs.lab1.mapreduce2.other.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author xushu
 * @create 8/9/21 9:46 PM
 * @desc worker 节点
 */
public class Worker {


    /**
     * 自身地址，ip：port
     */
    private String selfAddress;

    /**
     * coordinate 地址，ip：port
     */
    private String coordinatorAddress;


    /**
     * client
     */
    private RpcClient rpcClient;

    /**
     * 启动
     * @param ip
     * @param port
     */
    public void start(String ip, int port){
        this.selfAddress = ip + ":" + port;
        rpcClient = new RpcClient();
        rpcClient.init();

        pullTask();
    }

    /**
     * 向 master 拉取任务
     */
    private void pullTask() {

        while (true){
            try {
                Request request = Request.builder()
                        .messageType(MessageType.PULL_TASK)
                        .build();
                Response response = (Response) this.rpcClient.invokeSync(this.coordinatorAddress, request, 2000);
                Task task = (Task) response.getResult();
                switch (task.getTaskState()){
                    case Map:
                        handlerAssignMapTask(task);
                        break;
                    case Reduce:
                        handlerAssignReduceTask(task);
                        break;
                    case Wait:
                        Thread.sleep(5000);
                        break;
                    case Exit:
                        System.exit(-1);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }



    /**
     * 处理 map 任务
     * @return
     */
    private void handlerAssignMapTask(Task task)  {
        try {
            String fileName = task.getInput();
            List<KeyValue> keyValues = Wc.mapFunction(fileName);
            Map<String, List<KeyValue>> map = keyValues.stream().collect(Collectors.groupingBy(x -> x.getKey()));
            List<List<String>> reduceList = new ArrayList<>();
            for(int i = 0; i < task.getNReduce(); i++){
                reduceList.add(new ArrayList<>());
            }

            for (String key : map.keySet()) {
                List<KeyValue> values = map.get(key);
                String line = key + " " + values.stream().map(v -> v.getValue()).collect(Collectors.joining(","));

                //切分方式是根据key做hash，分成 R 份
                int index = Math.abs(key.hashCode() % task.getNReduce());
                List<String> subList = reduceList.get(index);
                subList.add(line);
            }


            List<String> intermediates = new ArrayList<>();
            for (int i = 0; i < reduceList.size(); i++) {
                String outPutFileName = "/Users/xushu/MyGithub Project/mit6824/src/main/java/labs/lab1/mapreduce2/file/mr_map_out_taskId" + task.getTaskNumber() + "_intermediate" + i + "_" + this.selfAddress + ".txt";
                File outFile = new File(outPutFileName);
                FileUtils.writeLines(outFile, reduceList.get(i));

                intermediates.add(outPutFileName);
            }

            task.setIntermediates(intermediates);

            // 通知 master 创建结束
            taskCompleted(task);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 处理 reduce 任务
     * @param task
     */
    private void handlerAssignReduceTask(Task task) {

        try {
            List<String> fileNames = task.getIntermediates();
            List<String> lines = new ArrayList<>();
            for (String fileName : fileNames) {
                Map<String, Integer> map = Wc.reduceFunction(fileName);
                for (String key : map.keySet()) {
                    String line = key + " " + map.get(key);
                    lines.add(line);
                }
            }

            String reduceFileName = "/Users/xushu/MyGithub Project/mit6824/src/main/java/labs/lab1/mapreduce2/file/mr_reduce_out_taskId" + task.getTaskNumber() + this.selfAddress  + ".txt";
            File outFile = new File(reduceFileName);
            FileUtils.writeLines(outFile, lines);

            task.setOutput(reduceFileName);

            taskCompleted(task);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 任务完成通知 master
     * @param task
     * @throws Exception
     */
    private void taskCompleted(Task task) throws Exception {
        Request request = Request.builder()
                .messageType(MessageType.TASK_COMPLETED)
                .obj(task)
                .build();

        this.rpcClient.invokeWithCallback(this.coordinatorAddress, request, null,3000);
    }


    public String getCoordinatorAddress() {
        return coordinatorAddress;
    }

    public void setCoordinatorAddress(String coordinatorAddress) {
        this.coordinatorAddress = coordinatorAddress;
    }
}
