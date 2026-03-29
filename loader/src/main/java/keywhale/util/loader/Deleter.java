package keywhale.util.loader;

public interface Deleter {
    void done();
    default void onNotFound() {}
}
