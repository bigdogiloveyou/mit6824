package labs.lab1.mapreduce2.main;

import labs.lab1.mapreduce2.Master;
import labs.lab1.mapreduce2.other.TaskStateEnum;

/**
 * @author xushu
 * @create 8/10/21 9:24 PM
 * @desc
 */
public class MrMaster {


    public static void main(String[] args) {

        Master master = new Master();


        String[] inputFiles = {
                "/Users/xushu/MyGithub Project/mit6824/src/main/java/labs/lab1/mapreduce2/file/pg-being_ernest.txt",
                "/Users/xushu/MyGithub Project/mit6824/src/main/java/labs/lab1/mapreduce2/file/pg-dorian_gray.txt",
                "/Users/xushu/MyGithub Project/mit6824/src/main/java/labs/lab1/mapreduce2/file/pg-frankenstein.txt",
                "/Users/xushu/MyGithub Project/mit6824/src/main/java/labs/lab1/mapreduce2/file/pg-grimm.txt",
                "/Users/xushu/MyGithub Project/mit6824/src/main/java/labs/lab1/mapreduce2/file/pg-huckleberry_finn.txt",
                "/Users/xushu/MyGithub Project/mit6824/src/main/java/labs/lab1/mapreduce2/file/pg-metamorphosis.txt",
                "/Users/xushu/MyGithub Project/mit6824/src/main/java/labs/lab1/mapreduce2/file/pg-sherlock_holmes.txt",
                "/Users/xushu/MyGithub Project/mit6824/src/main/java/labs/lab1/mapreduce2/file/pg-tom_sawyer.txt"
        };
        master.init(inputFiles, TaskStateEnum.Map, 10);
        master.start("localhost", 9992);
    }
}
