package com.sanketika.common.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class JsonUtil {

    private static final Logger logger = LoggerFactory.getLogger(JsonUtil.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private JsonUtil() {
        // Prevent instantiation
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> deserializeJson(String jsonString) {
        try {
            return mapper.readValue(jsonString, Map.class);
        } catch (Exception e) {
            logger.error("Error deserializing JSON: {}", e.getMessage());
            return new HashMap<>(); // Return empty map on failure
        }
    }

    public static <T> Either<Throwable, String> serialize(T obj) {
        try {
            return new Either.Right<>(mapper.writeValueAsString(obj));
        } catch (JsonProcessingException e) {
            logger.error("Error serializing JSON: {}", e.getMessage());
            return new Either.Left<>(e);
        }
    }

    // Either monad for handling serialization results
    public abstract static class Either<L, R> {
        private Either() {}

        public abstract boolean isLeft();
        public abstract boolean isRight();
        public abstract L left();
        public abstract R right();
        public abstract R getOrElse(R defaultValue);

        public static class Left<L, R> extends Either<L, R> {
            private final L value;

            Left(L value) {
                this.value = value;
            }

            @Override
            public boolean isLeft() {
                return true;
            }

            @Override
            public boolean isRight() {
                return false;
            }

            @Override
            public L left() {
                return value;
            }

            @Override
            public R right() {
                throw new UnsupportedOperationException("Cannot call right() on Left");
            }

            @Override
            public R getOrElse(R defaultValue) {
                return defaultValue;
            }
        }

        public static class Right<L, R> extends Either<L, R> {
            private final R value;

            Right(R value) {
                this.value = value;
            }

            @Override
            public boolean isLeft() {
                return false;
            }

            @Override
            public boolean isRight() {
                return true;
            }

            @Override
            public L left() {
                throw new UnsupportedOperationException("Cannot call left() on Right");
            }

            @Override
            public R right() {
                return value;
            }

            @Override
            public R getOrElse(R defaultValue) {
                return value;
            }
        }
    }
}
