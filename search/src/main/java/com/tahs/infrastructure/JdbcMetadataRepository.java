package com.tahs.infrastructure;

import com.tahs.application.ports.BookMetadata;
import com.tahs.application.ports.MetadataRepository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class JdbcMetadataRepository implements MetadataRepository {
    private final DataSource ds;

    public JdbcMetadataRepository(DataSource ds) {
        this.ds = ds;
    }

    @Override
    public BookMetadata findById(String bookId) {
        String sql = "SELECT book_id, title, author, year FROM books WHERE book_id = ?";
        try (Connection conn = ds.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, bookId);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return mapRowToBookMetadata(rs);
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error querying book by id: " + bookId, e);
        }
        return null;
    }

    @Override
    public List<BookMetadata> findByIds(List<String> bookIds) {
        if (bookIds.isEmpty()) {
            return List.of();
        }

        List<BookMetadata> results = new ArrayList<>();
        String placeholders = String.join(",", bookIds.stream().map(id -> "?").toList());
        String sql = "SELECT book_id, title, author, year FROM books WHERE book_id IN (" + placeholders + ")";

        try (Connection conn = ds.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < bookIds.size(); i++) {
                stmt.setString(i + 1, bookIds.get(i));
            }
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(mapRowToBookMetadata(rs));
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error querying books by ids", e);
        }
        return results;
    }

    private BookMetadata mapRowToBookMetadata(ResultSet rs) throws SQLException {
        return new BookMetadata(
                rs.getString("book_id"),
                rs.getString("title"),
                rs.getString("author"),
                rs.getInt("year")
        );
    }
}