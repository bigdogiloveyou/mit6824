package labs.lab1.mapreduce2;

import com.alipay.remoting.rpc.RpcClient;
import labs.lab1.mapreduce2.other.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
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
            File file = new File(fileName);
            if(!file.exists()){
                throw new Exception("文件不存在");
            }
            List<String> lines = FileUtils.readLines(file, "UTF-8");
            List<KeyValue> keyValues = Wc.mapFunction(lines);

            List<List<String>> reduceList = new ArrayList<>();
            for(int i = 0; i < task.getNReduce(); i++){
                reduceList.add(new ArrayList<>());
            }

            // 分成 R 份，这边可以执行 combiner 函数先合并好
            for (KeyValue keyValue : keyValues) {
                String line = keyValue.getKey() + " " + keyValue.getValue();
                //切分方式是根据key做hash，分成 R 份
                int index = Math.abs(keyValue.getKey().hashCode() % task.getNReduce());
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

            // 读取
            List<String> fileNames = task.getIntermediates();
            List<KeyValue> keyValues = new ArrayList<>();
            for (String fileName : fileNames) {
                File file = new File(fileName);
                if(!file.exists()){
                    throw new Exception("文件不存在");
                }
                List<String> lines = FileUtils.readLines(file, "UTF-8");
                for (String line : lines) {
                    String[] str = line.split(" ");
                    keyValues.add(KeyValue.builder().key(str[0]).value(str[1]).build());
                }
            }


            // 按照 key 生序
            keyValues.sort(new Comparator<KeyValue>() {
                @Override
                public int compare(KeyValue o1, KeyValue o2) {
                    return o1.getKey().compareTo(o2.getKey());
                }
            });

            List<String> outPutString = new ArrayList<>();
            int i = 0;
            while (i < keyValues.size()) {
                int j = i + 1;
                while (j < keyValues.size() && keyValues.get(i).getKey().equals(keyValues.get(j).getKey())){
                    j++;
                }
                List<KeyValue> subList = keyValues.subList(i, j - 1);
                outPutString.add(keyValues.get(i).getKey() + " " + Wc.reduceFunction(subList));

                i = j;
            }


            String reduceFileName = "/Users/xushu/MyGithub Project/mit6824/src/main/java/labs/lab1/mapreduce2/file/mr_reduce_out_taskId" + task.getTaskNumber() + this.selfAddress  + ".txt";
            File outFile = new File(reduceFileName);
            FileUtils.writeLines(outFile, outPutString);

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
