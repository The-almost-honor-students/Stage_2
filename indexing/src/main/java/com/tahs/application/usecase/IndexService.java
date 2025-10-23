package com.tahs.application.usecase;

import com.tahs.application.exceptions.BookNotFound;
import com.tahs.domain.Book;
import com.tahs.domain.BookSection;
import com.tahs.application.ports.InvertedIndexRepository;
import com.tahs.application.ports.MetadataRepository;
import com.tahs.application.dto.StatsDto;
import com.tahs.infrastructure.serialization.books.GutenbergHeaderSerializer;
import com.tahs.infrastructure.serialization.books.TextTokenizer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;

public class IndexService {

    private final MetadataRepository metadataRepository;
    private final GutenbergHeaderSerializer gutenbergHeaderSerializer;
    private final InvertedIndexRepository indexRepository;

    public IndexService(InvertedIndexRepository indexRepository, MetadataRepository metadataRepository,
                        GutenbergHeaderSerializer gutenbergHeaderSerializer) {
        this.indexRepository = indexRepository;
        this.metadataRepository = metadataRepository;
        this.gutenbergHeaderSerializer = gutenbergHeaderSerializer;
    }

    public void updateByBookId(String bookId) throws BookNotFound {
        try {
            updateMetadata(bookId);
        } catch (BookNotFound | IOException e) {
            throw new RuntimeException(e);
        }
        try {
            updateIndexWords(bookId);
        } catch (BookNotFound | IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void rebuildIndex() throws IOException {
        indexRepository.deleteAll();
        metadataRepository.deleteAll();
        Path cwd = Path.of("").toAbsolutePath().normalize();
        Path parent = cwd.getParent() != null ? cwd.getParent() : cwd;
        List<Path> roots = List.of(
                cwd.resolve("datalake")
        );
        for (Path root : roots) {
            if (!Files.exists(root)) continue;
            try (Stream<Path> stream = Files.find(
                    root,
                    3,
                    (p, attrs) -> attrs.isRegularFile()
                            && p.getFileName().toString().endsWith(".txt")
            )){
                stream.forEach(pathTxt -> {
                    try {
                        if (!Files.exists(pathTxt) &&
                                !Files.exists(Path.of(pathTxt.toString().replace("header", "body")))){
                            throw new NoSuchFileException("Not found header and body" + pathTxt.toString() + " in datalake");
                        }
                        var bookId = pathTxt.getFileName().toString().split("\\.")[0];
                        if (pathTxt.getFileName().toString().contains("header")){
                            var bookHeader = this.gutenbergHeaderSerializer.deserialize(pathTxt.toString());
                            metadataRepository.save(bookHeader);
                        }
                        if (pathTxt.getFileName().toString().contains("body")){
                            var text = this.gutenbergHeaderSerializer.readFile(pathTxt.toString());
                            var terms = TextTokenizer.extractTerms(text);
                            indexRepository.indexBook(bookId,terms);
                        }
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
        }
    }

    private void updateIndexWords(String bookId) throws IOException {
        var bookPath = findBookInDatalakeById(bookId, BookSection.BODY.fileSuffix());
        var text = this.gutenbergHeaderSerializer.readFile(bookPath);
        var terms = TextTokenizer.extractTerms(text);
        indexRepository.indexBook(bookId,terms);
    }

    private void updateMetadata(String bookId) throws IOException {
        var bookPath = findBookInDatalakeById(bookId, BookSection.HEADER.fileSuffix());
        var bookHeader = this.gutenbergHeaderSerializer.deserialize(bookPath);
        metadataRepository.save(bookHeader);
    }

    private String findBookInDatalakeById(String bookId,String bookSection) {
        if (bookId == null || bookId.isBlank()) {
            throw new IllegalArgumentException("bookId cannot be null or Empty");
        }

        var fileName = bookId + "." + bookSection + ".txt";
        Path cwd = Path.of("").toAbsolutePath().normalize();  // 👈 This is the current working directory
        Path parent = cwd.getParent() != null ? cwd.getParent() : cwd;

        List<Path> roots = List.of(
                cwd.resolve("datalake")
        );

        for (Path root : roots) {
            if (!Files.exists(root)) continue;

            try (Stream<Path> stream = Files.find(
                    root,
                    3,
                    (p, attrs) -> attrs.isRegularFile() && p.getFileName().toString().equals(fileName))) {

                var found = stream.findFirst();
                if (found.isPresent()) {
                    return found.get().toString();
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Error searching in " + root, e);
            }
        }

        throw new IllegalStateException(new NoSuchFileException("Not found " + fileName + " in datalake"));
    }

    public List<Book> getAllBooks() {
        return metadataRepository.getAll();
    }

    public StatsDto getStats() {
        var indexStats = indexRepository.getStats();
        return new StatsDto(
                metadataRepository.getAll().size(),
                indexStats.sizeMB(),
                indexStats.lastUpdate()
        );
    }
}
