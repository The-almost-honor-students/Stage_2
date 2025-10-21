package com.tahs.infrastructure.serialization.books;

import java.text.Normalizer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class TextTokenizer {
    private static final Pattern WORD = Pattern.compile("\\p{L}+(?:[’']\\p{L}+)*");

    private static final Set<String> STOP_WORDS = Set.of(
            "a", "about", "above", "after", "again", "against", "all", "am",
            "an", "and", "any", "are", "aren't", "as", "at", "be", "because",
            "been", "before", "being", "below", "between", "both", "but", "by",
            "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does",
            "doesn't", "doing", "don't", "down", "during", "each", "few", "for",
            "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't",
            "having", "he", "he'd", "he'll", "he's", "her", "here", "here's",
            "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd",
            "i'll", "i'm", "i've", "if", "in", "into", "is", "isn't", "it", "it's",
            "its", "itself", "let's", "me", "more", "most", "mustn't", "my",
            "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or",
            "other", "ought", "our", "ours", "ourselves", "out", "over", "own",
            "same", "shan't", "she", "she'd", "she'll", "she's", "should",
            "shouldn't", "so", "some", "such", "than", "that", "that's", "the",
            "their", "theirs", "them", "themselves", "then", "there", "there's",
            "these", "they", "they'd", "they'll", "they're", "they've", "this",
            "those", "through", "to", "too", "under", "until", "up", "very", "was",
            "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't",
            "what", "what's", "when", "when's", "where", "where's", "which", "while",
            "who", "who's", "whom", "why", "why's", "with", "won't", "would",
            "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your",
            "yours", "yourself", "yourselves"
    );

    private TextTokenizer() {}

    public static Set<String> extractTerms(String text) {
        if (text == null || text.isBlank()) return Collections.emptySet();

        String cleaned = normalize(text);
        Matcher m = WORD.matcher(cleaned);

        Set<String> terms = new HashSet<>();
        while (m.find()) {
            String w = m.group();
            if (!w.isBlank() && !isStopWord(w)) {
                terms.add(w);
            }
        }
        return terms;
    }

    private static boolean isStopWord(String w) {
        return STOP_WORDS.contains(w);
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