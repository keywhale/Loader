package keywhale.util.loader;

public interface Access<ID, VAL> {
    public VAL value();
    public ID id();
    public void done();
    public void save();
}
