package com.tahs.application.usecase;

import com.tahs.application.ports.BookMetadata;
import com.tahs.application.ports.IndexGateway;
import com.tahs.application.ports.MetadataRepository;
import com.tahs.application.ports.SearchHit;
import com.tahs.domain.RankedBook;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public class QueryBooksUseCase {

    private final IndexGateway indexGateway;
    private final MetadataRepository metadataRepository;

    public QueryBooksUseCase(IndexGateway indexGateway, MetadataRepository metadataRepository) {
        this.indexGateway = indexGateway;
        this.metadataRepository = metadataRepository;
    }

    public List<RankedBook> execute(String query) throws IOException, InterruptedException, SQLException {
        List<SearchHit> hits = indexGateway.search(query);

        List<BookMetadata> metadata = metadataRepository.findByIds(
                hits.stream().map(SearchHit::getBookId).toList()
        );

        return hits.stream()
                .map(hit -> {
                    BookMetadata meta = metadata.stream()
                            .filter(m -> m.getBookId().equals(hit.getBookId()))
                            .findFirst()
                            .orElseThrow();
                    double finalScore = adjustScore(hit.getScore(), meta);
                    return new RankedBook(meta, finalScore);
                })
                .toList();
    }

    private double adjustScore(double score, BookMetadata meta) {
        return score; // opcional
    }
}
