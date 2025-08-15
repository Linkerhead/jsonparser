package ru.hofftech.omni.pimmigrator.model;

import lombok.Builder;

@Builder
public record ProductPimAttribute(String attributePimId, String name, String type, String value) {
}
