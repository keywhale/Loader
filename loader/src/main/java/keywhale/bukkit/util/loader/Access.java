package keywhale.bukkit.util.loader;

public interface Access<ID, VAL> {
    public VAL value();
    public ID id();
    public void done();
}
