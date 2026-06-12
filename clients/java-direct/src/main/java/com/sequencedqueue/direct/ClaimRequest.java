package com.sequencedqueue.direct;

import java.util.List;

public record ClaimRequest(String workerId, List<String> supportedItemTypes, Integer leaseSeconds, Integer maxItems) {
}
