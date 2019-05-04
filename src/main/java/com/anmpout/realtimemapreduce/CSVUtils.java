
package com.anmpout.realtimemapreduce;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

public class CSVUtils {

    private static final char DS= '\t';
    
    public static void writeLine(Writer w, List<String> values) throws IOException {
        writeLine(w, values, DS, ' ');
    }

    private static String followformat(String value) {

        String result = value;
        if (result.contains("\"")) {
            result = result.replace("\"", "\"\"");
        }
        return result;

    }

    public static void writeLine(Writer w, List<String> values, char separators, char customQuote) throws IOException {

        boolean first = true;
        if (separators == ' ') {
            separators = DS;
        }

        StringBuilder sb = new StringBuilder();
        for (String value : values) {
            if (!first) {
                sb.append(separators);
            }
            if (customQuote == ' ') {
                sb.append(followformat(value));
            } else {
                sb.append(customQuote).append(followformat(value)).append(customQuote);
            }

            first = false;
        }
        sb.append("\n");
        w.append(sb.toString());


    }

}
