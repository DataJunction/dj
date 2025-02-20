package io.datajunction.models.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class SQLResponse {
    private String sql;
    private List<Column> columns;
    private String dialect;
    private List<String> upstreamTables;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Column {
        private String name;
        private String type;
        private String column;
        private String node;
        private String semanticEntity;
        private String semanticType;
    }
}