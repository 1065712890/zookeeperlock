package cn.dengbin97.lock.lock;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.*;

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
    private static CuratorFramework client;

    private static final String LOCK_KEY = "/DB_ZOOKEEPER_LOCK_";
    private static final String SUFFIX = "/LOCK";

    private static ExecutorService executor = Executors.newFixedThreadPool(20);

    static {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client =
                CuratorFrameworkFactory.newClient(
                        "localhost:2181,localhost:2182,localhost:2183",
                        5000,
                        3000,
                        retryPolicy);
        client.start();
    }

    /**
     * @description: 加锁
     * @author: dengbin
     * @date: 2018/11/14 下午2:43
     */
    public static String lock(String key, String value) throws Exception {
        //拼接节点全路径
        String nodeName = key + SUFFIX;
        String res = client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(nodeName, value.getBytes());
        log.info("res:{}", res);
        while (true) {
            //获取父节点下的全部子节点，判断自己是否是第一个，若不是，则监听自己前面的节点删除事件
            List<String> children = client.getChildren().forPath(key);
            Collections.sort(children);
            if (res.contains(children.get(0))) {
                log.info("success {}", res);
                return res;
            } else {
                log.info("加锁失败 waiting...");
                //此处使用countDownLatch来实现，主线程await，当监听到事件后唤醒这个线程重新去获取锁
                CountDownLatch countDownLatch = new CountDownLatch(1);
                int index = getIndex(res, children);
                String pre = children.get(index - 1);
                log.info("index:{} pre:{}", index, pre);
                try {
                    client.getData().usingWatcher((Watcher) watchedEvent -> countDownLatch.countDown()).forPath(key + "/" + pre);
                    countDownLatch.await();
                } catch (Exception e) {
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
    public static int getIndex(String nodeName, List<String> children) {
        for (int i = 0; i < children.size(); ++i) {
            if (nodeName.contains(children.get(i))) {
                return i;
            }
        }
        return -1;
    }


    /**
     * @description: 尝试加锁
     * @author: dengbin
     * @date: 2018/11/20 下午5:32
     */
    public static String tryLock(String productId, String requestId, Integer times) throws ExecutionException {
        log.info("productId:{} requestId:{} times:{}", productId, requestId, times);
        String lockKey = generateLockKey(productId);
        String res = null;
        //通过callable和futureTask来执行加锁，可以定时
        Future<String> task = executor.submit(() -> lock(lockKey, productId));
        try {
            //times为超时时间，单位为秒
            res = task.get(times, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.info("timeout productId:{} requestId:{} times:{} res:{}", productId, requestId, times, res);
        } catch (InterruptedException e) {
        }
        return res;
    }

    public static void main(String[] args) {
        for (int i = 0; i < 10; ++i) {
            executor.execute(() -> {
                String res;
                try {
                    res = tryLock("999", "dengbin", 5000);
                    if (res != null) {
                        Thread.sleep(2000);
                        unLock(res);
                    } else {
                        System.out.println("error");
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });

        }
    }

    /**
     * @description: 解锁
     * @author: dengbin
     * @date: 2018/11/14 下午3:21
     */
    public static Boolean unLock(String nodeName) {
        try {
            client.delete().forPath(nodeName);
            log.info("delete node-{}", nodeName);
        } catch (Exception e) {
            log.error("", e);
        }
        return true;
    }

    /**
     * @description: 生成锁的路径
     * @author: dengbin
     * @date: 2018/11/20 下午4:58
     */
    private static String generateLockKey(String productId) {
        return LOCK_KEY + productId;
    }


}
