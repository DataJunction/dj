package io.datajunction.client;

import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.datajunction.models.requests.Catalog;
import io.datajunction.models.requests.Dimension;
import io.datajunction.models.requests.Engine;
import io.datajunction.models.requests.Metric;
import io.datajunction.models.requests.Source;
import io.datajunction.models.requests.Transform;
import io.datajunction.models.responses.CatalogResponse;
import io.datajunction.models.responses.CommonDimensionsResponse;
import io.datajunction.models.responses.DimensionLinkResponse;
import io.datajunction.models.responses.EngineResponse;
import io.datajunction.models.responses.NamespaceResponse;
import io.datajunction.models.responses.NodeResponse;
import io.datajunction.models.responses.SQLResponse;

public class DJClient {

    private final HttpClient client;
    private final String baseURL;
    private final ObjectMapper objectMapper;
    private String cookie;

    public DJClient(HttpClient client, String baseURL) {
        this.client = client;
        this.baseURL = baseURL;
        this.objectMapper = new ObjectMapper();
        this.cookie = "";
    }

    private CompletableFuture<HttpResponse<String>> sendRequest(HttpRequest request) {
        return client.sendAsync(request, BodyHandlers.ofString());
    }

    private String toJson(Object object) {
        try {
            return objectMapper.writeValueAsString(object);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize object to JSON", e);
        }
    }

    private HttpRequest buildRequest(String endpoint, String method, String body) {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(baseURL + endpoint))
                .header("Content-Type", "application/json");

        if (!cookie.isEmpty()) {
            builder.header("Cookie", cookie);
        }

        switch (method) {
            case "POST":
                if (body != null) {
                    builder.POST(BodyPublishers.ofString(body));
                } else {
                    builder.POST(BodyPublishers.noBody());
                }
                break;
            case "PATCH":
                if (body != null) {
                    builder.method("PATCH", BodyPublishers.ofString(body));
                } else {
                    builder.method("PATCH", BodyPublishers.noBody());
                }
                break;
            case "GET":
            default:
                builder.GET();
                break;
        }

        return builder.build();
    }

    public CompletableFuture<Void> login(String username, String password) {
        Map<String, String> formData = Map.of(
                "username", username,
                "password", password
        );
        String form = formData.entrySet().stream()
                .map(entry -> URLEncoder.encode(entry.getKey(), StandardCharsets.UTF_8) + "=" + URLEncoder.encode(entry.getValue(), StandardCharsets.UTF_8))
                .collect(Collectors.joining("&"));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(baseURL + "/basic/login/"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .POST(BodyPublishers.ofString(form))
                .build();

        return sendRequest(request).thenAccept(response -> {
            if (response.statusCode() != 200) {
                throw new RuntimeException("Login failed: " + response.statusCode() + " " + response.body());
            }

            List<String> cookies = response.headers().allValues("Set-Cookie");
            if (!cookies.isEmpty()) {
                this.cookie = cookies.get(0);
            }
        });
    }

    public CompletableFuture<CatalogResponse> createCatalog(Catalog catalog) {
        String body = toJson(catalog);
        HttpRequest request = buildRequest("/catalogs/", "POST", body);
        return sendRequest(request).thenApply(response -> {
            try {
                return objectMapper.readValue(response.body(), CatalogResponse.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize response", e);
            }
        });
    }

    public CompletableFuture<EngineResponse> createEngine(Engine engine) {
        String body = toJson(engine);
        HttpRequest request = buildRequest("/engines/", "POST", body);
        return sendRequest(request).thenApply(response -> {
            try {
                return objectMapper.readValue(response.body(), EngineResponse.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize response", e);
            }
        });
    }

    public CompletableFuture<EngineResponse> addEngineToCatalog(String catalog, Engine engine) {
        String body = toJson(List.of(engine));
        HttpRequest request = buildRequest("/catalogs/" + catalog + "/engines/", "POST", body);
        return sendRequest(request).thenApply(response -> {
            try {
                return objectMapper.readValue(response.body(), EngineResponse.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize response", e);
            }
        });
    }

    public CompletableFuture<NamespaceResponse> createNamespace(String namespace) {
        HttpRequest request = buildRequest("/namespaces/" + namespace + "/", "POST", null);
        return sendRequest(request).thenApply(response -> {
            try {
                return objectMapper.readValue(response.body(), NamespaceResponse.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize response", e);
            }
        });
    }

    public CompletableFuture<NodeResponse> registerTable(String catalog, String schema, String table) {
        HttpRequest request = buildRequest("/register/table/" + catalog + "/" + schema + "/" + table, "POST", null);
        return sendRequest(request).thenApply(response -> {
            try {
                return objectMapper.readValue(response.body(), NodeResponse.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize response", e);
            }
        });
    }

    public CompletableFuture<NodeResponse> createSource(Source source) {
        String body = toJson(source);
        HttpRequest request = buildRequest("/nodes/source/", "POST", body);
        return sendRequest(request).thenApply(response -> {
            try {
                return objectMapper.readValue(response.body(), NodeResponse.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize response", e);
            }
        });
    }

    public CompletableFuture<NodeResponse> createTransform(Transform transform) {
        String body = toJson(transform);
        HttpRequest request = buildRequest("/nodes/transform/", "POST", body);
        return sendRequest(request).thenApply(response -> {
            try {
                return objectMapper.readValue(response.body(), NodeResponse.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize response", e);
            }
        });
    }

    public CompletableFuture<NodeResponse> createDimension(Dimension dimension) {
        String body = toJson(dimension);
        HttpRequest request = buildRequest("/nodes/dimension/", "POST", body);
        return sendRequest(request).thenApply(response -> {
            try {
                return objectMapper.readValue(response.body(), NodeResponse.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize response", e);
            }
        });
    }

    public CompletableFuture<DimensionLinkResponse> linkDimensionToNode(String nodeName, String nodeColumn, String dimension, String dimensionColumn) {
        HttpRequest request = buildRequest(
                "/nodes/" + nodeName + "/columns/" + nodeColumn + "/?dimension=" + dimension + "&dimension_column=" + dimensionColumn,
                "POST",
                null
        );
        return sendRequest(request).thenApply(response -> {
            try {
                return objectMapper.readValue(response.body(), DimensionLinkResponse.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize response", e);
            }
        });
    }

    public CompletableFuture<NodeResponse> createMetric(Metric metric) {
        String body = toJson(metric);
        HttpRequest request = buildRequest("/nodes/metric/", "POST", body);
        return sendRequest(request).thenApply(response -> {
            try {
                return objectMapper.readValue(response.body(), NodeResponse.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize response", e);
            }
        });
    }

    public CompletableFuture<List<CommonDimensionsResponse.Dimension>> listCommonDimensions(List<String> metrics) {
        String metricsQuery = "?" + String.join("&", metrics.stream().map(m -> "metric=" + m).toList());
        HttpRequest request = buildRequest("/metrics/common/dimensions/" + metricsQuery, "GET", null);
        return sendRequest(request).thenApply(response -> {
            try {
                return objectMapper.readValue(response.body(), objectMapper.getTypeFactory().constructCollectionType(List.class, CommonDimensionsResponse.Dimension.class));
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize response", e);
            }
        });
    }

    public CompletableFuture<SQLResponse> getSQL(List<String> metrics, List<String> dimensions, List<String> filters) {
        String metricsQuery = "?" + String.join("&", metrics.stream().map(m -> "metrics=" + m).toList());
        String dimensionsQuery = String.join("&", dimensions.stream().map(d -> "dimensions=" + d).toList());
        String filtersQuery = String.join("&", filters.stream().map(f -> "filters=" + f).toList());
        HttpRequest request = buildRequest("/sql/" + metricsQuery + "&" + dimensionsQuery + filtersQuery, "GET", null);
        return sendRequest(request).thenApply(response -> {
            try {
                return objectMapper.readValue(response.body(), SQLResponse.class);
            } catch (Exception e) {
                throw new RuntimeException("Failed to deserialize response", e);
            }
        });
    }
}