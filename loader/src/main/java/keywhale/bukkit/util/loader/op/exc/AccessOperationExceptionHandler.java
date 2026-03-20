package keywhale.bukkit.util.loader.op.exc;

public interface AccessOperationExceptionHandler {

    public default void handleDefault(Exception exc) {
        //
    }
 
    public default void handlePart1(Part1AccessOperationException exc) {
        this.handleDefault(exc);
    }

    public default void handlePart1(RuntimeException exc) {
        this.handleDefault(exc);
    }

    public default void handlePart2(Part2AccessOperationException exc) {
        this.handleDefault(exc);
    }

    public default void handlePart2(RuntimeException exc) {
        this.handleDefault(exc);
    }

}
