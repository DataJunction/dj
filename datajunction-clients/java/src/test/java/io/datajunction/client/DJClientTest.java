package io.datajunction.client;

import io.datajunction.models.requests.Catalog;
import io.datajunction.models.requests.Column;
import io.datajunction.models.requests.Dimension;
import io.datajunction.models.requests.Engine;
import io.datajunction.models.requests.Metric;
import io.datajunction.models.requests.Source;
import io.datajunction.models.requests.Transform;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.http.HttpClient;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class DJClientTest {

    private static DJClient dj;

    @BeforeAll
    public static void setUp() throws Exception {
        HttpClient client = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        dj = new DJClient(client, "http://localhost:8000");

        int maxRetries = 5;
        int retryDelay = 5;
        boolean loginSuccessful = false;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                dj.login("dj", "dj").join();
                loginSuccessful = true;
                break;
            } catch (Exception e) {
                System.err.println("Login attempt " + attempt + " failed: " + e.getMessage());
                if (attempt < maxRetries) {
                    System.out.println("Retrying in " + retryDelay + " seconds...");
                    TimeUnit.SECONDS.sleep(retryDelay);
                    retryDelay *= 2;
                } else {
                    throw new RuntimeException("Failed to log in after " + maxRetries + " attempts", e);
                }
            }
        }

        if (!loginSuccessful) {
            throw new RuntimeException("Failed to log in after " + maxRetries + " attempts");
        }
    }

    @Test
    public void shouldReturnSomething() throws Exception {
        CompletableFuture<Void> testFlow = dj.createCatalog(new Catalog("tpch"))
                .thenCompose(response -> dj.createEngine(new Engine("trino", "451")))
                .thenCompose(response -> dj.addEngineToCatalog("tpch", new Engine("trino", "451")))
                .thenCompose(response -> dj.createNamespace("integration.tests.trino"))
                .thenCompose(response -> dj.createSource(new Source(
                        "integration.tests.source1",
                        "unknown",
                        "db",
                        "tbl",
                        "Test Source with Columns",
                        "A test source node with columns",
                        List.of(
                                new Column("id", "int"),
                                new Column("name", "string"),
                                new Column("price", "double"),
                                new Column("created_at", "timestamp")
                        ),
                        List.of("id"),
                        "published",
                        true
                )))
                .thenCompose(response -> dj.registerTable("tpch", "sf1", "orders"))
                .thenCompose(response -> dj.createTransform(new Transform(
                        "integration.tests.trino.transform1",
                        "Filter to last 1000 records",
                        "The last 1000 purchases",
                        "published",
                        "select custkey, totalprice, orderdate from source.tpch.sf1.orders order by orderdate desc limit 1000",
                        true
                )))
                .thenCompose(response -> dj.createDimension(new Dimension(
                        "integration.tests.trino.dimension1",
                        "Customer keys",
                        "All custkey values in the source table",
                        "published",
                        List.of("id"),
                        List.of(),
                        "select custkey as id, 'attribute' as foo from source.tpch.sf1.orders",
                        true
                )))
                .thenCompose(response -> dj.linkDimensionToNode(
                        "integration.tests.trino.transform1",
                        "custkey",
                        "integration.tests.trino.dimension1",
                        "id"
                ))
                .thenCompose(response -> dj.createMetric(new Metric(
                        "integration.tests.trino.metric1",
                        "Total of last 1000 purchases",
                        "This is the total amount from the last 1000 purchases",
                        "published",
                        "select sum(totalprice) from integration.tests.trino.transform1",
                        true
                )))
                .thenCompose(response -> dj.listCommonDimensions(List.of("integration.tests.trino.metric1")))
                .thenCompose(response -> dj.getSQL(
                        List.of("integration.tests.trino.metric1"),
                        List.of("integration.tests.trino.dimension1.id"),
                        List.of()
                ))
                .thenAccept(query -> assertTrue(query.getSql().contains("SELECT")));

        testFlow.join();
    }
}
