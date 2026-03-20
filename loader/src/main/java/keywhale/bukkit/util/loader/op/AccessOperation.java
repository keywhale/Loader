package keywhale.bukkit.util.loader.op;

public interface AccessOperation<ID, VAL> {
    public VAL value();
    public ID id();

    // true: Found or created
    // false: Not found
    /*
    Upon the *found/created* completion of part 1, the ID is expected to
    become available via `id()`.
    */
    public boolean runPart1(); // ASYNC
    /*
    Upon the completion of part 2, the value is expected to become available
    via `value()`.
    */
    public void runPart2(); // SYNC
}