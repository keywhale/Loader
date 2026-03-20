package keywhale.bukkit.util.loader.op;

import keywhale.bukkit.util.loader.op.exc.Part1SaveOperationException;
import keywhale.bukkit.util.loader.op.exc.Part2SaveOperationException;

public interface SaveOperation {
    public void runPart1() throws Part1SaveOperationException; // SYNC
    public void runPart2() throws Part2SaveOperationException; // ASYNC
}
