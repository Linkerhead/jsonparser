package ru.hofftech.omni.pimmigrator.model;

import lombok.Builder;

@Builder
public record RawAttribute(String attributePimId,
                           String name,
                           String type,
                           String value) {
}
