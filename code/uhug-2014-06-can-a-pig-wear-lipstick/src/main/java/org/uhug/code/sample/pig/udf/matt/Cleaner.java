package org.uhug.code.sample.pig.udf.matt;

/**
 *
 * @author matt davies
 */
public class Cleaner {

    private String suffix = null;

    public Cleaner(String suffix) {
        this.suffix = suffix;
    }

    public String cleanWord( String input) {
        int start = 0;
        int end = -1;
        if (null != input) {
            char[] chars = input.toCharArray();

            // find the beginning of the word
            for (int i = 0; i < chars.length; i++) {
                int ascii = (int) chars[i];
                if ((ascii >= 65 && ascii <= 90) || (ascii >= 97 && ascii <= 122)) {
                    start = i;
                    break;
                }
            }

            //find the end of the word
            for (int i = chars.length; i > 0; i--) {
                int ascii = (int) chars[i - 1];
                if ((ascii >= 65 && ascii <= 90) || (ascii >= 97 && ascii <= 122)) {
                    end = i;
                    break;
                }
            }
        }

        if (end > 0) {
            return input.substring(start, end) + (null == suffix ? "" : suffix);
        } else {
            return null;
        }

    }
}
