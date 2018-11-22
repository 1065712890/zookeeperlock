import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

/**
 * @program: zookeeperlock
 * @description: 测试
 * @author: dengbin
 * @create: 2018-11-19 19:39
 **/

public class Test {


    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        ZooKeeper zk = new ZooKeeper("127.0.0.1:2181", 2000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(111);
            }
        });
        System.out.println(new String(zk.getData("/temp", true, new Stat())));

        Thread.sleep(10000);
        zk.close();
    }
}
