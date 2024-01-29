package dev.tungtv;

import java.util.concurrent.*;

public class Main {

    public static void main(String[] args) {
//        ExecutorService executor = Executors.newFixedThreadPool(5);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                 5, 7, 0L,
                 TimeUnit.MILLISECONDS,
                 new ArrayBlockingQueue<>(10));


            for(int i = 0; i < 100; i++) {
                executor.execute(new RequestHandler("req-" + i));
                System.out.println(executor.getQueue());
            }
        executor.shutdown(); // Không cho threadpool nhận thêm nhiệm vụ nào nữa

        while (!executor.isTerminated()) {
                System.out.println("TUNGTV");
            }
    }
}
