package keywhale.bukkit.util.loader.op;

import keywhale.bukkit.util.loader.op.exc.Part1AccessOperationException;
import keywhale.bukkit.util.loader.op.exc.Part2AccessOperationException;

public interface AccessOperation<ID, VAL> {
    public VAL value();
    public ID id();

    // true: Found or created
    // false: Not found
    /*
    Upon the *found/created* completion of part 1, the ID is expected to
    become available via `id()`.
    */
    public boolean runPart1() throws Part1AccessOperationException; // ASYNC
    /*
    Upon the completion of part 2, the value is expected to become available
    via `value()`.
    */
    public void runPart2() throws Part2AccessOperationException; // SYNC
}