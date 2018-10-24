import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;

/**
 * Utility class, common to both csv and sql
 */
public class Util {

    public static void prettyPrint(String[] headers) {
        String content = Arrays.stream(headers).collect(Collectors.joining(", "));
        System.out.println(content);
    }

    static final int MAX = 27;

    public static String toHeader(String h) {
        String base = h.toLowerCase().replaceAll("_", "").replaceAll("\\s", "");
        if (base.length() > MAX)
            return base.substring(0, MAX);
        else
            return base.toLowerCase();
    }

    static Set<String> toLowerCaseSet(String[] arr) {
        return Arrays.stream(arr).map((e -> toHeader(e))).collect(Collectors.toSet());
    }

    public static Sets.SetView<String> compareHeaders(String[] arrayA, String[] arrayB) {
        Set<String> setA = toLowerCaseSet(arrayA);
        Set<String> setB = toLowerCaseSet(arrayB);
        return Sets.difference(setA, setB);
    }
}