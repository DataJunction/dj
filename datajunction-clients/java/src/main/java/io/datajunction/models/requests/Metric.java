package io.datajunction.models.requests;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class Metric {
    private String name;
    @JsonProperty("display_name")
    private String displayName;
    private String description;
    private String mode;
    private String query;
    @JsonProperty("update_if_exists")
    private boolean updateIfExists;

    public Metric(String name, String displayName, String description, String mode,
                  String query, boolean updateIfExists) {
        this.name = name;
        this.displayName = displayName;
        this.description = description;
        this.mode = mode;
        this.query = query;
        this.updateIfExists = updateIfExists;
    }
}