import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @program: zookeeperlock
 * @description: 测试zookeeper相关
 * @author: dengbin
 * @create: 2018-11-19 17:34
 **/

public class DemoTest {
    public static CountDownLatch countDownLatch = new CountDownLatch(100);

    public static void main(String[] args) throws Exception {
        Runnable runnable = new Runnable(){

            /**
             * When an object implementing interface <code>Runnable</code> is used
             * to create a thread, starting the thread causes the object's
             * <code>run</code> method to be called in that separately executing
             * thread.
             * <p>
             * The general contract of the method <code>run</code> is that it may
             * take any action whatsoever.
             *
             * @see Thread#run()
             */
            @Override
            public void run() {
                try {
                    play();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        long start = System.currentTimeMillis();
        for(int i = 0; i < 100; ++i){
            Thread t = new Thread(runnable);
            t.join();
            t.start();
        }
        countDownLatch.await();
        long end = System.currentTimeMillis();
        System.out.println(start);
        System.out.println(end);
        System.out.println((end - start) / 1000);
    }

    public static void play() throws Exception {
        //创建zookeeper的客户端
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);

        CuratorFramework client = CuratorFrameworkFactory.newClient("127.0.0.1:2181", retryPolicy);

        client.start();

//创建分布式锁, 锁空间的根节点路径为/curator/lock


        InterProcessMutex mutex = new InterProcessMutex(client, "/lock");

        mutex.acquire();

//获得了锁, 进行业务流程

//完成业务流程, 释放锁

        mutex.release();

//关闭客户端

        client.close();

        countDownLatch.countDown();
    }
}
