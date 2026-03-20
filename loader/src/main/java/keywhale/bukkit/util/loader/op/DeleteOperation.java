package keywhale.bukkit.util.loader.op;

import keywhale.bukkit.util.loader.op.exc.DeleteOperationException;

public interface DeleteOperation {
    public void run() throws DeleteOperationException;
}
