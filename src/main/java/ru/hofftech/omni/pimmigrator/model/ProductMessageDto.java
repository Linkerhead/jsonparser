package ru.hofftech.omni.pimmigrator.model;

import lombok.Builder;

import java.util.List;

@Builder
public record ProductMessageDto(String productId, List<ProductPimAttribute> attributes) {
}
