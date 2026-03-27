package keywhale.bukkit.util.loader.op;

public interface OperationExceptionHandler<ID, VAL> {

    public default void handleDefault(Exception exc) {
        //
    }

    // Save

    public default void handleSavePart1(OperationException exc, ID id, VAL value) {
        this.handleDefault(exc);
    }

    public default void handleSavePart1(RuntimeException exc, ID id, VAL value) {
        this.handleDefault(exc);
    }

    public default void handleSavePart2(OperationException exc, ID id, VAL value) {
        this.handleDefault(exc);
    }

    public default void handleSavePart2(RuntimeException exc, ID id, VAL value) {
        this.handleDefault(exc);
    }

    // Delete

    public default void handleDelete(OperationException exc, ID id) {
        this.handleDefault(exc);
    }

    public default void handleDelete(RuntimeException exc, ID id) {
        this.handleDefault(exc);
    }

    // Access

    public default void handleAccessPart1(OperationException exc) {
        this.handleDefault(exc);
    }

    public default void handleAccessPart1(RuntimeException exc) {
        this.handleDefault(exc);
    }

    public default void handleAccessPart2(OperationException exc, ID id) {
        this.handleDefault(exc);
    }

    public default void handleAccessPart2(RuntimeException exc, ID id) {
        this.handleDefault(exc);
    }

}
