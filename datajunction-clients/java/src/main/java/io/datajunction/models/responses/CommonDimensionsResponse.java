package io.datajunction.models.responses;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import lombok.Data;
import java.util.List;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class CommonDimensionsResponse {
    private List<Dimension> dimensions;

    @Data
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Dimension {
        private String name;
        private String nodeName;
        private String nodeDisplayName;
        private List<String> properties;
        private String type;
        private List<String> path;
        private boolean filterOnly;
    }
}