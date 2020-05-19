import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.util.Queue;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WebScraperThread extends Thread {
    private Pattern emailExpression;
    private Set<String> visited;
    private Queue<String> toVisit;
    private Set<String> emailSet;

    WebScraperThread(Set<String> visited, Queue<String> toVisit, Set<String> emailSet) {
        this.visited = visited;
        this.toVisit = toVisit;
        this.emailSet = emailSet;
        //regex for emails
        emailExpression = Pattern.compile("[a-zA-Z0-9_.+-]+@[a-zA-Z0-9]+\\.[a-zA-Z]{2,4}");
    }

    public void run() {
        Document doc;
        Elements links;
        Matcher emailMatcher;
        try {
            if (toVisit.size() < 1)
                sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        while (emailSet.size() < 10000) {
            try {
                doc = Jsoup.connect(toVisit.remove()).get();

                //get links and add to HashSet and queue
                links = doc.select("a[href]");
                for (Element link : links) {
                    if (visited.add(link.attr("abs:href")))
                        toVisit.add(link.attr("abs:href"));
                }

                //get emails and add to HashSet
                emailMatcher = emailExpression.matcher(doc.text());
                while (emailMatcher.find()) {
                    emailSet.add(emailMatcher.group());
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}