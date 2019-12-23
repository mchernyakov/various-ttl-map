package com.github.mchernyakov.variousttlmap.util;


import org.jetbrains.annotations.NotNull;

public final class Preconditions {
    private Preconditions() {

    }

    public static void checkArgument(boolean expression) {
        if (!expression) {
            throw new IllegalArgumentException();
        }
    }

    public static <T extends @NotNull Object> T checkNotNull(T reference) {
        if (reference == null) {
            throw new NullPointerException();
        }
        return reference;
    }
}
