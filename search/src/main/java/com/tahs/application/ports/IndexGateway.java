package com.tahs.application.ports;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

public interface IndexGateway {
    List<SearchHit> search(String query) throws IOException, InterruptedException, SQLException;
}