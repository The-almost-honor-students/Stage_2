package com.tahs.infrastructure.serialization.books;

import com.tahs.domain.Book;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Pattern;

public class GutenbergHeaderSerializer {
    private static final String RX_TITLE = "^Title:\\s*(.+)$";
    private static final String RX_AUTHOR = "^Author:\\s*(.+)$";
    private static final String RX_LANGUAGE = "^Language:\\s*(.+)$";
    private static final String RX_EBOOK_ID = "\\[(?:[^\\]]*?)\\b(?:e[-\\s]?book|ebook)\\s*#\\s*(\\d+)\\b[^\\]]*?\\]";
    private static final String RX_EBOOK_ID_FALLBACK = "\\b(?:project\\s+gutenberg.*?)?\\b(?:e[-\\s]?book|ebook)\\s*#\\s*(\\d+)\\b";

    public GutenbergHeaderSerializer() {
    }

    public Book deserialize(String givenABookHeaderPath) throws IOException {
        var readTextBook = readFile(givenABookHeaderPath);
        return deserializeBookFromText(readTextBook);
    }

    public String readFile(String text) throws IOException {
        return Files.readString(Path.of(text), StandardCharsets.UTF_8);
    }

    private Book deserializeBookFromText(String readTextBook) {
        var title = extractFirstRegex(RX_TITLE,readTextBook);
        var author =  extractFirstRegex(RX_AUTHOR,readTextBook);
        var language = extractFirstRegex(RX_LANGUAGE,readTextBook);
        var bookId = extractBookId(readTextBook);
        return new Book(bookId,title,author,language);
    }

    private String extractFirstRegex(String regex, String text) {
        var pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
        var m = pattern.matcher(text);
        if (m.find()) {
            return m.group(1);
        }
        return "";
    }

    private int extractBookId(String text) {
        var id = extractFirstRegex(RX_EBOOK_ID, text);
        if (id == null) {
            id = extractFirstRegex(RX_EBOOK_ID_FALLBACK, text);
        }
        try {
            return id != null ? Integer.parseInt(id) : 0;
        } catch (NumberFormatException ignored) {
            return 0;
        }
    }
}
