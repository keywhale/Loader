package keywhale.bukkit.util.loader.op.exc;

public interface DeleteOperationExceptionHandler {

    public default void handleDefault(Exception exc) {
        //
    }

    public default void handle(DeleteOperationException exc) {
        this.handleDefault(exc);
    }

    public default void handle(RuntimeException exc) {
        this.handleDefault(exc);
    }

}
