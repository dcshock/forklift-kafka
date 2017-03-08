package forklift.retry;

@FunctionalInterface
public interface Callback<V> {
    void handle(V value);
}
