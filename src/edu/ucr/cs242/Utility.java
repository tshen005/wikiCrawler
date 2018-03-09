package edu.ucr.cs242;

import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.time.Duration;
import java.time.LocalDateTime;

public class Utility {
    public static void waitThreads(Thread[] threads) {
        for (Thread thread : threads) {
            if (thread != null) {
                // Wait threads to exit.
                try { thread.join(); }
                catch (InterruptedException e) { thread.interrupt(); }
            }
        }
    }

    public static int calculatePartition(int numOfPages, int numOfThreads, int threadId) {
        int tasksPerThread = numOfPages / numOfThreads;
        tasksPerThread += threadId < (numOfPages % numOfThreads) ? 1 : 0;
        return tasksPerThread;
    }

    public static String elapsedTime(LocalDateTime start, LocalDateTime end) {
        Duration elapsed = Duration.between(start, end);

        long hours = elapsed.toHours();
        long minutes = elapsed.toMinutes() % 60;
        long seconds = elapsed.getSeconds() % 60;
        long milliseconds = elapsed.toMillis() % 1000;

        return String.format("%02d:%02d:%02d.%03d", hours, minutes, seconds, milliseconds);
    }

    public static boolean openOutputLog(String logOutput) {
        if (logOutput != null) {
            try {
                PrintStream ps = new PrintStream(new BufferedOutputStream(new FileOutputStream(logOutput)), true);
                System.setOut(ps);
                return true;
            } catch (FileNotFoundException e) {
                return false;
            }
        }

        return true;
    }
}
