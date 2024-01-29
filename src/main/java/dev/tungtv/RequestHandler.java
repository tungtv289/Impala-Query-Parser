package dev.tungtv;

public class RequestHandler implements Runnable{
    private String name;

    public RequestHandler(String name) {
        this.name = name;
    }

    @Override
    public void run() {
        try {
            System.out.println(Thread.currentThread().getName() + " Starting process req " + name);
            Thread.sleep(500);
            System.err.println(Thread.currentThread().getName() + " Finished process req " + name);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
