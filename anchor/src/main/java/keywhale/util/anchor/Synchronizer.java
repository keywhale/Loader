package keywhale.util.anchor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class Synchronizer {

    private final Object defaultKey = new Object();
    private final Map<Object, Entry> locks = new HashMap<>();

    private static class Entry {
        final ReentrantLock lock = new ReentrantLock();
        int refs = 0;
    }

    public interface Lock extends AutoCloseable {
        @Override
        void close();
    }

    public Lock lock(Object key) {
        ReentrantLock rl;
        synchronized (locks) {
            Entry entry = locks.computeIfAbsent(key, k -> new Entry());
            entry.refs++;
            rl = entry.lock;
        }
        rl.lock();
        return () -> {
            rl.unlock();
            synchronized (locks) {
                Entry entry = locks.get(key);
                if (--entry.refs == 0) locks.remove(key);
            }
        };
    }

    public Lock lock() {
        return this.lock(this.defaultKey);
    }

}
