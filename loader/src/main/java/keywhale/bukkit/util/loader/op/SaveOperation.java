package keywhale.bukkit.util.loader.op;

public interface SaveOperation {
    public void runPart1(); // SYNC
    public void runPart2(); // ASYNC
}
