package labs.lab1.mapreduce2.main;

import labs.lab1.mapreduce2.Worker;

/**
 * @author xushu
 * @create 8/10/21 9:25 PM
 * @desc
 */
public class MrWorker3 {


    public static void main(String[] args) {
        Worker worker = new Worker();
        worker.setCoordinatorAddress("localhost:9992");
        worker.start("localhost", 12002);
    }
}
