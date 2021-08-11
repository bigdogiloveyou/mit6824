package labs.lab1.mapreduce2;

import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.RpcClient;
import com.alipay.remoting.rpc.RpcServer;
import labs.lab1.mapreduce2.other.*;
import labs.lab1.mapreduce2.other.TaskMetaInfo;
import labs.lab1.mapreduce2.other.TaskMetaInfoEnum;
import labs.lab1.mapreduce2.other.Task;
import labs.lab1.mapreduce2.other.TaskStateEnum;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author xushu
 * @create 8/9/21 9:46 PM
 * @desc
 */
public class Master {


    private RpcServer rpcServer;

    private RpcClient rpcClient;

    private Object lock = new Object();


    /**
     * 所有等待执行的 task
     */
    private LinkedBlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();

    /**
     * task 元信息
     */
    private Map<Integer, TaskMetaInfo> taskMeta = new HashMap<>();

    /**
     * master 当前所处阶段
     */
    private TaskStateEnum masterPhase;

    private int nReduce;

    private String[] inputFiles;

    /**
     * Map任务产生的R个中间文件的信息
     */
    private Map<Integer, List<String>> intermediates = new HashMap<>();


    public void init(String[] inputFiles, TaskStateEnum masterPhase, int nReduce) {
        this.inputFiles = inputFiles;
        this.masterPhase = masterPhase;
        this.nReduce = nReduce;
    }

    public void start(String ip, int port){
        // 启动 server
        rpcServer = new RpcServer(port, true, false);

        rpcServer.registerUserProcessor(new MessageProcessor<Request>() {

            @Override
            public Object handleRequest(BizContext bizCtx, Request request)  {
                return handlerRequest(request);
            }
        });

        rpcServer.start(ip);

        rpcClient = new RpcClient();
        rpcClient.init();


        // 创建 mapTask
        createMapTask();


        catcheTimeOutTask();
    }

    /**
     * 处理 worker 请求，分为拉取任务与完成任务
     * @param req
     * @return
     */
    public Response handlerRequest(Request req){
        switch (req.getMessageType()){
            case PULL_TASK:
                return assginTask();
            case TASK_COMPLETED:
                taskCompleted((Task)req.getObj());
        }

        return null;
    }

    /**
     * 处理超时异常
     */
    private void catcheTimeOutTask() {
        while(true){
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            synchronized (lock){
                if(TaskStateEnum.Exit.equals(this.masterPhase)){
                    return;
                }

                // 超时则设为空闲，重新拉取
                for (Integer taskIndex : taskMeta.keySet()) {
                    TaskMetaInfo taskMetaInfo = taskMeta.get(taskIndex);
                    if(TaskMetaInfoEnum.InProgress.equals(taskMetaInfo.getTaskMetaInfoEnum())
                            && (System.currentTimeMillis() - taskMetaInfo.getStartTime()) / 1000 > 10){

                        // 超时了重新放进去
                        this.taskQueue.add(taskMetaInfo.getTask());
                        taskMetaInfo.setTaskMetaInfoEnum(TaskMetaInfoEnum.Idle);
                    }
                }
            }
        }
    }


    /**
     * 创建 mapTask
     */
    private void createMapTask() {

        for (int i = 0; i < this.inputFiles.length; i++) {
            // 具体每个任务
            Task task = Task.builder()
                    .input(this.inputFiles[i])
                    .taskState(TaskStateEnum.Map)
                    .nReduce(this.nReduce)
                    .TaskNumber(i)
                    .build();

            taskQueue.add(task);

            // 每个任务对应的信息
            TaskMetaInfo masterTask = TaskMetaInfo.builder()
                    .taskMetaInfoEnum(TaskMetaInfoEnum.Idle)
                    .task(task)
                    .build();
            taskMeta.put(i, masterTask);
        }
    }


    /**
     * 处理完成的任务
     * @param task
     */
    private void taskCompleted(Task task) {
        synchronized (lock){
            TaskMetaInfo taskMetaInfo = this.taskMeta.get(task.getTaskNumber());
            // 不是此阶段的任务或者任务已经完成，则忽略
            if(!task.getTaskState().equals(this.masterPhase) || TaskMetaInfoEnum.Completed.equals(taskMetaInfo.getTaskMetaInfoEnum())){
                return;
            }

            this.taskMeta.get(task.getTaskNumber()).setTaskMetaInfoEnum(TaskMetaInfoEnum.Completed);
            switch (task.getTaskState()){
                case Map:
                    // map 任务的结果以任务 id 分开
                    for (int i = 0; i < task.getIntermediates().size(); i++) {
                        List<String> filePath = this.intermediates.getOrDefault(i, new ArrayList<>());
                        filePath.add(task.getIntermediates().get(i));
                        this.intermediates.put(i, filePath);
                    }

                    // 所有任务完成才进入 reduce 阶段
                    if(allTaskDone()){
                        createReduceTask();
                        this.masterPhase = TaskStateEnum.Reduce;
                    }
                    break;
                case Reduce:
                    // reduce 所有任务完成就退出
                    if(allTaskDone()){
                        this.masterPhase = TaskStateEnum.Exit;
                    }
                    break;
            }
        }
    }


    /**
     * 创建 reduce 任务
     */
    private void createReduceTask() {
        this.taskMeta.clear();
        this.taskQueue.clear();
        for (Integer key : this.intermediates.keySet()) {
            // 具体每个任务
            Task task = Task.builder()
                    .taskState(TaskStateEnum.Reduce)
                    .nReduce(this.nReduce)
                    .TaskNumber(key)
                    .intermediates(this.intermediates.get(key))
                    .build();

            taskQueue.add(task);

            // 每个任务对应的信息
            TaskMetaInfo masterTask = TaskMetaInfo.builder()
                    .taskMetaInfoEnum(TaskMetaInfoEnum.Idle)
                    .task(task)
                    .build();
            taskMeta.put(key, masterTask);
        }
    }


    /**
     * 所有的任务都已经完成
     * @return
     */
    private boolean allTaskDone() {
        for (Integer key : this.taskMeta.keySet()) {
            if(!TaskMetaInfoEnum.Completed.equals(this.taskMeta.get(key).getTaskMetaInfoEnum())){
                return false;
            }
        }
        return true;
    }

    /**
     * 分配任务
     * @return
     */
    private Response assginTask() {

        synchronized (lock){
            if(taskQueue.size() > 0){
                // 有就发出去
                Task task = taskQueue.poll();
                //记录启动时间
                TaskMetaInfo masterTask = taskMeta.get(task.getTaskNumber());
                masterTask.setStartTime(System.currentTimeMillis());
                masterTask.setTaskMetaInfoEnum(TaskMetaInfoEnum.InProgress);

                return Response.builder()
                        .result(task)
                        .build();
            }else if(TaskStateEnum.Exit.equals(this.masterPhase)){
                return Response.builder()
                        .result(Task.builder().taskState(TaskStateEnum.Exit).build())
                        .build();
            }else {
                // 没有 task 就让 worker 等待
                return Response.builder()
                        .result(Task.builder().taskState(TaskStateEnum.Wait).build())
                        .build();
            }
        }
    }


}
