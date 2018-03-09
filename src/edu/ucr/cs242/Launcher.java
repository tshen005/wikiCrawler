package edu.ucr.cs242;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class Launcher {
    private static Map<String, Subroutine> subroutines = new HashMap<>();

    static {
        subroutines.put("crawler",
                new Subroutine("crawler",
                        "edu.ucr.cs242.crawler.WikiCrawler",
                        "execute the Wikipedia crawler"));
        subroutines.put("indexer",
                new Subroutine("indexer",
                        "edu.ucr.cs242.indexing.Indexer",
                        "execute the Lucene indexer"));
    }

    private static void printMessage(String message) {
        System.out.println("cs242: " + message);
    }

    private static void printUsage() {
        System.out.println("usage: cs242 <subroutine> [options] <arguments...>");
        System.out.println("possible subroutines:");
        subroutines.forEach((key, value) -> System.out.format(" %-10s%s%n", key, value.getDescription()));
        System.exit(1);
    }

    public static void main(String[] args) throws Exception {
        if (args.length <= 0) {
            printMessage("subroutine is not specified");
            printUsage();
        }

        if (!subroutines.containsKey(args[0])) {
            printMessage("invalid subroutine: " + args[0]);
            printUsage();
        }

        Class<?> clazz = Class.forName(subroutines.get(args[0]).getClassName());
        Method method = clazz.getMethod("main", String[].class);
        method.invoke(null, (Object) Arrays.stream(args).skip(1).toArray(String[]::new));
    }
}
