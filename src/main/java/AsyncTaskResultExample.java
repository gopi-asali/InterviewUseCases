import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class AsyncTaskResultExample {
    public static void main(String[] args) {
        ExecutorService executor = Executors.newFixedThreadPool(3);

        // Submit tasks for execution and receive Future objects
        Future<Integer> future1 = executor.submit(new Task(1));
        Future<Integer> future2 = executor.submit(new Task(2));
        Future<Integer> future3 = executor.submit(new Task(3));

        // Shutdown the executor when no more tasks will be submitted
        executor.shutdown();

        try {
            // Retrieve and print results when tasks are completed
            System.out.println("Result from Task 1: " + future1.get());
            System.out.println("Result from Task 2: " + future2.get());
            System.out.println("Result from Task 3: " + future3.get());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static class Task implements java.util.concurrent.Callable<Integer> {
        private int taskId;

        public Task(int taskId) {
            this.taskId = taskId;
        }

        @Override
        public Integer call() throws InterruptedException {
            System.out.println("Task " + taskId + " is running on thread " + Thread.currentThread().getName());

            Thread.sleep(1000);
            return taskId * 10;
        }
    }
}
