package com.tahs.infrastructure;

import com.tahs.application.usecase.QueryBooksUseCase;
import com.tahs.domain.RankedBook;
import io.javalin.Javalin;

import java.util.List;

public class QueryEngineHttpServer {

    public static void main(String[] args) {
        var indexGateway = new JdbcInvertedIndexGateway(MyDataSource.get());
        var metadataRepo = new JdbcMetadataRepository(MyDataSource.get());
        var queryUseCase = new QueryBooksUseCase(indexGateway, metadataRepo);

        var app = Javalin.create().start(9090);

        app.get("/query", ctx -> {
            try {
                String q = ctx.queryParam("q");
                if (q == null || q.trim().isEmpty()) {
                    ctx.status(400).json(new ErrorResponse("Query parameter 'q' is required"));
                    return;
                }

                List<RankedBook> results = queryUseCase.execute(q);
                ctx.json(results);
            } catch (Exception e) {
                ctx.status(500).json(new ErrorResponse("Search error: " + e.getMessage()));
                e.printStackTrace();
            }
        });

        System.out.println("Query Engine started on http://localhost:9090");
    }

    public static class ErrorResponse {
        public String error;

        public ErrorResponse(String error) {
            this.error = error;
        }
    }
}