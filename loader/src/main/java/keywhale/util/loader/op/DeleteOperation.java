package keywhale.util.loader.op;

public interface DeleteOperation {
    /*
    `start` may be called from any thread. Accesses pending while the delete was
    in progress are re-issued via their AccessOperation after the delete completes,
    so the callback thread does not determine where Accessor.init runs.
    */
    public void start(Runnable callback);
}
