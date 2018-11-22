import lombok.extern.slf4j.Slf4j;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * @program: zookeeperlock
 * @description: zookeeper分布式锁
 * @author: dengbin
 * @create: 2018-11-20 16:52
 **/

@Slf4j
public class ZookeeperLock {
    private static ZooKeeper zk;

    private static final String LOCK_KEY = "/DB_ZOOKEEPER_LOCK_";
    private static final String SUFFIX = "/LOCK";

    static {
        try {
            zk = new ZooKeeper("127.0.0.1:2181", 2000, new Watcher() {
                @Override
                public void process(WatchedEvent watchedEvent) {
                    System.out.println("zookeeper分布式锁");
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @description: 加锁
     * @author: dengbin
     * @date: 2018/11/14 下午2:43
     */
    public static String lock(String key, String value) throws KeeperException, InterruptedException {
        //拼接节点全路径
        String nodeName = key + SUFFIX;
        String res = zk.create(nodeName, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        while(true){
            //获取父节点下的全部子节点，判断自己是否是第一个，若不是，则监听自己前面的节点删除事件
            List<String> children = zk.getChildren(key, false);
            Collections.sort(children);
            log.info("", children);
            if(res.indexOf(children.get(0)) != -1){
                log.info("success {}", res);
                return res;
            }else{
                log.info("加锁失败 waiting...");
                //此处使用countdownlatch来实现，主线程await，当监听到事件后唤醒这个线程重新去获取锁
                CountDownLatch countDownLatch = new CountDownLatch(1);
                int index = getIndex(res, children);
                String pre = children.get(index - 1);
                log.info("index:{} pre:{}", index, pre);
                try {
                    zk.getData(key + "/" + pre, new Watcher() {
                        @Override
                        public void process(WatchedEvent watchedEvent) {
                            countDownLatch.countDown();;
                        }
                    }, new Stat());
                    countDownLatch.await();
                }catch (Exception e){
                    log.error("", e);
                    continue;
                }
            }
        }

    }

    /**
    * @description: 获取当前节点在整个子节点中的位置
    * @author: dengbin
    * @date: 2018/11/21 下午2:14
    */
    public static int getIndex(String nodeName, List<String> children){
        for(int i = 0; i < children.size(); ++i){
            if(nodeName.indexOf(children.get(i)) != -1){
                return  i;
            }
        }
        return -1;
    }


    /**
    * @description: 尝试加锁
    * @author: dengbin
    * @date: 2018/11/20 下午5:32
    */
    public static String tryLock(String productId, String requestId, Integer times) throws InterruptedException, KeeperException, ExecutionException {
        log.info("productId:{} requestId:{} times:{}", productId, requestId, times);
        String lockKey = generateLockKey(productId);
        //判断加锁的父节点是否存在，若不存在，则先新建
        if(zk.exists(lockKey, false) == null){
            zk.create(lockKey, lockKey.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            log.info("create node {}", lockKey);
        }
        String res = null;
        //通过callable和futuretask来执行加锁，可以定时
        FutureTask<String> task = new FutureTask<>(new Callable<String>() {
            @Override
            public String call() throws Exception {
                return lock(lockKey, productId);
            }
        });
        new Thread(task).start();
        try {
            //times为超时时间，单位为秒
            res = task.get(times, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.info("timeout productId:{} requestId:{} times:{} res:{}", productId, requestId, times, res);
            e.printStackTrace();
        }
        return res;
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException, KeeperException {
//        String res = tryLock("123", "dengbin", 10);
//
        for(int i = 0; i < 10; ++i){
            new Thread(new Runnable() {
                @Override
                public void run() {
                    String res = null;
                    try {
                        res = tryLock("123", "dengbin", 500);
                        if(res != null){
                            Thread.sleep(2000);
                            unLock(res);
                        }else {
                            System.out.println("error");
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (ExecutionException e) {
                        e.printStackTrace();
                    }

                }
            }).start();
        }
    }

    /**
     * @description: 解锁
     * @author: dengbin
     * @date: 2018/11/14 下午3:21
     */
    public static Boolean unLock(String nodeName) throws KeeperException, InterruptedException {
        try {
            zk.delete(nodeName, 0);
            log.info("delete node-{}", nodeName);
        }catch (Exception e){
            log.error("", e);
        }
        return true;
    }

    /**
    * @description: 生成锁的路径
    * @author: dengbin
    * @date: 2018/11/20 下午4:58
    */
    private static String generateLockKey(String productId){
        return LOCK_KEY + productId;
    }


}
