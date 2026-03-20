package keywhale.bukkit.util.loader.op.exc;

public interface SaveOperationExceptionHandler {

    public default void handleDefault(Exception exc) {
        //
    }

    public default void handlePart1(Part1SaveOperationException exc) {
        this.handleDefault(exc);
    }

    public default void handlePart1(RuntimeException exc) {
        this.handleDefault(exc);
    }

    public default void handlePart2(Part2SaveOperationException exc) {
        this.handleDefault(exc);
    }

    public default void handlePart2(RuntimeException exc) {
        this.handleDefault(exc);
    }
    
}
