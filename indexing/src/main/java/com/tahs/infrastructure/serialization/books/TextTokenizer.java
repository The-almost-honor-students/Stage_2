package com.tahs.infrastructure.serialization.books;

import java.text.Normalizer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TextTokenizer {
    private static final Pattern WORD = Pattern.compile("\\p{L}+(?:[’']\\p{L}+)*");

    private TextTokenizer() {}

    public static Set<String> extractTerms(String text) {
        if (text == null || text.isBlank()) return Collections.emptySet();

        String cleaned = normalize(text);
        Matcher m = WORD.matcher(cleaned);

        Set<String> terms = new HashSet<>();
        while (m.find()) {
            String w = m.group();
            if (!w.isBlank()) terms.add(w);
        }
        return terms;
    }

    private static String normalize(String s) {
        String unified = s
                .replace('\u2019', '\'') // ’
                .replace('\u2018', '\'') // ‘
                .replace('\u201B', '\'') // ‛
                .replace('\u2032', '\'') // ′
                .replace('\u00B4', '\'') // ´
                .replace('\u201C', '"')  // “
                .replace('\u201D', '"'); // ”
        String nfd = Normalizer.normalize(unified, Normalizer.Form.NFD);
        String noMarks = nfd.replaceAll("\\p{M}", "");
        return noMarks.toLowerCase(Locale.ROOT);
    }
}