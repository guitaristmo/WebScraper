import java.util.Collections;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ThreadLauncher {

    public static void main(String[] args) {
        Set<String> emailAddressHashSet = Collections.synchronizedSet(new HashSet<>());
        Set<String> visitedLinkSet = Collections.synchronizedSet(new HashSet<>());
        Queue<String> linksToBeVisitedQueue = new ConcurrentLinkedQueue<>();
        String DBURL = "jdbc:sqlserver://spring2019touro.cbjmpwcdjfmq.us-east-1.rds.amazonaws.com;"
                + "database=Ioffe364;"
                + "user=++++++++;"
                + "password=+++++++++;"
                + "encrypt=false;"
                + "trustServerCertificate=false;"
                + "loginTimeout=30;";

        linksToBeVisitedQueue.add("https://www.touro.edu/");
        ExecutorService threadPool = Executors.newFixedThreadPool(100);

        for (int thread = 1; thread <= 100; thread++) {
            threadPool.execute(new WebScraperThread(visitedLinkSet, linksToBeVisitedQueue, emailAddressHashSet));
        }

        try {
            threadPool.shutdown();
            threadPool.awaitTermination(60, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        DBConnectivity dbConnection = new DBConnectivity(DBURL);
        dbConnection.HashSetToDB(emailAddressHashSet);
    }
}