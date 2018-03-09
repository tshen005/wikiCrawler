package edu.ucr.cs242.crawler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class RobotPolicy {
    /**
     * Robots.txt matching pattern.
     */
    public static final String PATTERN = "User-agent: ([^#]*)|Allow: ([^#]*)|Disallow: ([^#]*)";
    private static final Pattern pattern = Pattern.compile(PATTERN, Pattern.CASE_INSENSITIVE);

    private String userAgent;
    // LinkedHashMap keep the insertion order
    private Map<String, Boolean> accessControlMap = new LinkedHashMap<>();
    private URL robotURL;

    public RobotPolicy(String userAgent) {
        this.userAgent = userAgent;
    }

    public boolean parse(URL url) {
        HttpURLConnection connection = null;
        try {
            robotURL = new URL(url, "/robots.txt");
            connection = (HttpURLConnection) robotURL.openConnection();
            connection.setRequestMethod("GET");
            connection.connect();

            // Not 200? Assume no robots.txt enforced.
            if (connection.getResponseCode() != 200)
                return true;

            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            boolean uaMatched = false;

            while ((line = reader.readLine()) != null) {
                Matcher matcher = pattern.matcher(line.trim());
                if (matcher.find()) {
                    // either group 1, 2, 3 is not null.
                    if (matcher.group(1) != null) {
                        String regex = createRegexFromWildcard(matcher.group(1).trim());
                        uaMatched = userAgent.matches(regex);
                    } else if (uaMatched && matcher.group(2) != null) {
                        String path = URLDecoder.decode(matcher.group(2), "UTF-8").trim();
                        accessControlMap.put(path, true);
                    } else if (uaMatched && matcher.group(3) != null) {
                        String path = URLDecoder.decode(matcher.group(3), "UTF-8").trim();
                        accessControlMap.put(path, false);
                    }
                }
            }
            return true;
        } catch (IOException e) {
            return false;
        } finally {
            if (connection != null)
                connection.disconnect();
        }
    }

    public boolean testURL(URL url) {
        // No robotURL, assume every url is fine.
        if (robotURL == null)
            return true;

        // If host matches, check our policy list.
        if (url.getHost().equals(robotURL.getHost())) {
            return accessControlMap.entrySet().stream()
                    .filter(p -> url.getFile().startsWith(p.getKey()))
                    .map(Map.Entry::getValue)
                    .findFirst().orElse(true);
        }

        // Not the same host, assume allowed.
        return true;
    }

    private String createRegexFromWildcard(String wildcard) {
        // String.chars() returns an IntStream
        return wildcard.chars().mapToObj(_c -> {
            // map char to string
            char ch = (char) _c;

            switch (ch) {
                case '*': return ".*";
                case '?': return ".";

                case '(': case ')': case '[': case ']': case '$':
                case '^': case '.': case '{': case '}': case '|':
                case '\\':
                    return "\\" + ch;

                default:
                    return String.valueOf(ch);
            }
        }).collect(Collectors.joining("", "^", "$"));
    }
}
