/**
 * @program: zookeeperlock
 * @description: 测试线程join
 * @author: dengbin
 * @create: 2018-11-21 14:30
 **/

public class MyTest {

    public static void t() throws InterruptedException {
        Thread.sleep(5000);
        System.out.println("background thread end");
    }

    public static void main(String[] args) throws InterruptedException {
        Thread th = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    t();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
        th.start();
        th.join(2000);
        System.out.println("end");
    }
}
