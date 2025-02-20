package io.datajunction.models.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class NodeResponse {
    private String namespace;
    private int nodeRevisionId;
    private int nodeId;
    private String type;
    private String name;
    private String displayName;
    private String version;
    private String status;
    private String mode;
    private Catalog catalog;
    @JsonProperty("schema_")
    private String schema;
    private String table;
    private String description;
    private String query;
    private List<Column> columns;
    private String updatedAt;
    private List<Parent> parents;
    private String createdAt;
    private CreatedBy createdBy;
    private List<String> tags;
    private String currentVersion;
    private boolean missingTable;
    private Object customMetadata;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Catalog {
        private String name;
        private List<EngineResponse> engines;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Column {
        private String name;
        private String displayName;
        private String type;
        private List<Object> attributes;
        private Object dimension;
        private Object partition;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Parent {
        private String name;
    }

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CreatedBy {
        private String username;
    }
}