package com.tahs.application.usecase;

import com.tahs.application.ports.InvertedIndexRepository;
import com.tahs.application.ports.MetadataRepository;
import com.tahs.infrastructure.serialization.books.GutenbergHeaderSerializer;

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

    public void updateByBookId(String bookId) throws IOException {
        updateMetadata(bookId);
//        indexRepository.indexBook(bookId);
    }

    private void updateMetadata(String bookId) throws IOException {
        var bookPath = findBookInDatalakeById(bookId);
        var bookHeader =this.gutenbergHeaderSerializer.deserialize(bookPath);
        metadataRepository.save(bookHeader);
    }

    private String findBookInDatalakeById(String bookId) {
        if (bookId == null || bookId.isBlank()) {
            throw new IllegalArgumentException("bookId no puede ser nulo o vacío");
        }

        var fileName = bookId + ".header.txt";
        Path cwd = Path.of("").toAbsolutePath().normalize();  // 👈 This is the current working directory
        Path parent = cwd.getParent() != null ? cwd.getParent() : cwd;

        List<Path> roots = List.of(
                cwd.resolve("datalake"),
                cwd.resolve("isdatalake"),
                parent.resolve("datalake"),
                parent.resolve("isdatalake")
        );

        for (Path root : roots) {
            if (!Files.exists(root)) continue;

            try (Stream<Path> stream = Files.find(
                    root,
                    3, // datalake/YYYYMMDD/HH/archivo
                    (p, attrs) -> attrs.isRegularFile() && p.getFileName().toString().equals(fileName))) {

                var found = stream.findFirst();
                if (found.isPresent()) {
                    return found.get().toString();
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Error al buscar en " + root, e);
            }
        }

        throw new IllegalStateException(new NoSuchFileException("No se encontró " + fileName + " en el datalake"));
    }

}
