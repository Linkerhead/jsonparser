package ru.hofftech.omni.pimmigrator.model;

import lombok.Builder;

import java.util.List;

@Builder
public record RawProduct(String productId,
                         List<RawAttribute> attributes) {
}
