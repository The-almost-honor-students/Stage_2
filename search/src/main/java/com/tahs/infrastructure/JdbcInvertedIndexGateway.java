package com.tahs.infrastructure;

import com.tahs.application.ports.IndexGateway;
import com.tahs.application.ports.SearchHit;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class JdbcInvertedIndexGateway implements IndexGateway {
    private final DataSource ds;

    public JdbcInvertedIndexGateway(DataSource ds) {
        this.ds = ds;
    }

    @Override
    public List<SearchHit> search(String query) throws SQLException {
        String[] words = query.toLowerCase().split("\\s+");
        Map<String, Double> bookScores = new HashMap<>();

        for (String word : words) {
            List<SearchHit> hits = searchWord(word);
            for (SearchHit hit : hits) {
                bookScores.merge(hit.getBookId(), hit.getScore(), Double::sum);
            }
        }

        return bookScores.entrySet().stream()
                .map(entry -> new SearchHit(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparingDouble(SearchHit::getScore).reversed())
                .toList();
    }

    private List<SearchHit> searchWord(String word) throws SQLException {
        List<SearchHit> results = new ArrayList<>();
        String sql = "SELECT book_id, frequency FROM inverted_index WHERE word = ?";

        try (Connection conn = ds.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, word);
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String bookId = rs.getString("book_id");
                    double score = rs.getDouble("frequency");
                    results.add(new SearchHit(bookId, score));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error searching word: " + word, e);
        }

        return results;
    }
}