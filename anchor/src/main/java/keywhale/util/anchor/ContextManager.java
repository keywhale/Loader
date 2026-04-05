package keywhale.util.anchor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class ContextManager {

    private final Object lock = new Object();
    private ContextTracker tracker = null;

    public interface Context {
        public void done();
    }

    private final class ContextTracker {
        public ContextTracker next;
        public Consumer<Context> start;
        public Context ctx;

        public void start() {
            this.start.accept(this.ctx);
        }
    }
    
    public void enter(Consumer<Context> start) {
        synchronized (this.lock) {
            AtomicBoolean done = new AtomicBoolean();
            ContextTracker trk = new ContextTracker();
            trk.start = start;
            trk.ctx = () -> {
                synchronized (this.lock) {
                    if (done.get()) return;
                    done.set(true);
                    
                    if (trk.next == null) {
                        this.tracker = null;
                    } else {
                        trk.next.start();
                    }
                }
            };

            if (this.tracker == null) {
                this.tracker = trk;
                trk.start();
            } else {
                this.tracker.next = trk;
                this.tracker = trk;
            }
        }
    }
}
