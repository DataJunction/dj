package io.datajunction.client;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

public class DJClientTest {
    static DJClient client;

    @BeforeAll
    public static void init() {
        client = new DJClient();
    }

    @Test
    public void testListMetrics() throws IOException, InterruptedException {
        String metrics = client.listMetrics();
        assertEquals(metrics, "[\"default.num_repair_orders\",\"default.avg_repair_price\",\"default.total_repair_cost\",\"default.avg_length_of_employment\",\"default.total_repair_order_discounts\",\"default.avg_repair_order_discounts\",\"default.avg_time_to_dispatch\"]");
    }
}
