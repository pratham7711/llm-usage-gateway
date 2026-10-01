package io.github.pratham7711.llmgw.gateway;

public record Tenant(String id, int ratePerSec, int burst, long monthlyTokenQuota) {}
