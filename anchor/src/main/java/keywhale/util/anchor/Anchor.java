package keywhale.util.anchor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;

public class Anchor<ID, VAL> {

    private final Object lock = new Object();
    private final Event shutdown = new Event();
    private final Core<ID, VAL> core;
    private final Map<ID, Tracker<ID, VAL>> trackers = new HashMap<>();

    public Anchor(Core<ID, VAL> core) {
        this.core = core;
    }

    private static <E> E pop(Set<E> set) {
        E e = set.iterator().next();
        set.remove(e);
        return e;
    }

    public interface Core<ID, VAL> {
        class NotFoundException extends RuntimeException {}
        default VAL load(ID id) { throw new UnsupportedOperationException(); }
        default void save(ID id, VAL val) { throw new UnsupportedOperationException(); }
        default void delete(ID id) { throw new UnsupportedOperationException(); }
    }

    public static class Event {
        private final Object lock = new Object();
        private boolean set = false;

        public void set() {
            synchronized (this.lock) { set = true; lock.notifyAll(); }
        }

        public boolean isSet() {
            synchronized (this.lock) { return set; }
        }

        public void await() {
            synchronized (this.lock) {
                while (!set) {
                    try { lock.wait(); }
                    catch (InterruptedException ignored) { if (set) return; }
                }
            }
        }
    }

    public interface Access<ID, VAL> extends AutoCloseable {
        ID id();
        VAL value();
        void save();
        @Override void close();
        Event getCancelEvent();
    }

    public static class ShuttingDownException extends IllegalStateException {}

    private void checkShutdown() {
        if (shutdown.isSet()) throw new ShuttingDownException();
    }

    public Access<ID, VAL> access(ID id) {
        Event onCancel = new Event();
        Supplier<Access<ID, VAL>> s;
        synchronized (this.lock) {
            checkShutdown();
            s = trackers.computeIfAbsent(id, k -> new Tracker<>(this, k)).access(onCancel);
        }
        return s.get();
    }

    public void delete(ID id) {
        Runnable r;
        synchronized (this.lock) {
            checkShutdown();
            r = trackers.computeIfAbsent(id, k -> new Tracker<>(this, k)).delete();
        }
        r.run();
    }

    public void shutdown() {
        synchronized (this.lock) {
            if (shutdown.isSet()) return;
            shutdown.set();
            for (Tracker<ID, VAL> t : trackers.values()) t.shutdown();
        }
    }

    // ---- Tracker ----

    private static class Tracker<ID, VAL> {
        final Anchor<ID, VAL> lc;
        final ID id;

        interface State {}

        State state = null;
        final Set<Park<Access<ID, VAL>, Event>> pendingAccess = new HashSet<>();
        final Set<Park<Void, Void>> pendingDelete = new HashSet<>();

        Tracker(Anchor<ID, VAL> lc, ID id) {
            this.lc = lc;
            this.id = id;
        }

        // ---- State classes ----

        static class LoadingState implements State {
            boolean hasRunner = false;
        }

        static class ActiveState<ID, VAL> implements State {
            final Tracker<ID, VAL> tracker;
            final VAL value;
            boolean cancelling = false;
            volatile boolean anyRequiresSave = false;
            final List<Event> accessors = new ArrayList<>();

            ActiveState(Tracker<ID, VAL> tracker, VAL value) {
                this.tracker = tracker;
                this.value = value;
            }

            Access<ID, VAL> provision(Event onCancel) {
                accessors.add(onCancel);
                return new Access<ID, VAL>() {
                    boolean closed = false;

                    @Override public ID id() { return tracker.id; }
                    @Override public VAL value() { return ActiveState.this.value; }
                    @Override public void save() { ActiveState.this.anyRequiresSave = true; }

                    @Override
                    public void close() {
                        Runnable post;
                        synchronized (tracker.lc.lock) {
                            if (closed) return;
                            closed = true;
                            post = ActiveState.this.done(onCancel);
                        }
                        post.run();
                    }

                    @Override
                    public Event getCancelEvent() {
                        return onCancel;
                    }
                };
            }

            // Called under lock.
            Runnable done(Event onCancel) {
                accessors.remove(onCancel);
                if (!accessors.isEmpty()) return () -> {};

                if (tracker.lc.shutdown.isSet()) {
                    tracker.state = null;
                    tracker.lc.trackers.remove(tracker.id);
                    return () -> {};
                }

                if (!tracker.pendingDelete.isEmpty()) {
                    tracker.startDeleting();
                    pop(tracker.pendingDelete).resume();
                    return () -> {};
                }

                if (anyRequiresSave) {
                    tracker.state = new SavingState();
                    VAL val = this.value;
                    return () -> {
                        try {
                            tracker.lc.core.save(tracker.id, val);
                        } catch (RuntimeException ex) {
                            synchronized (tracker.lc.lock) {
                                if (!tracker.pendingDelete.isEmpty()) {
                                    tracker.startDeleting();
                                    pop(tracker.pendingDelete).resume();
                                } else if (!tracker.pendingAccess.isEmpty()) {
                                    tracker.state = new LoadingState();
                                    pop(tracker.pendingAccess).resume();
                                } else {
                                    tracker.state = null;
                                    tracker.lc.trackers.remove(tracker.id);
                                }
                            }
                            throw ex;
                        }
                        synchronized (tracker.lc.lock) {
                            if (!tracker.pendingDelete.isEmpty()) {
                                tracker.startDeleting();
                                pop(tracker.pendingDelete).resume();
                            } else if (!tracker.pendingAccess.isEmpty()) {
                                ActiveState<ID, VAL> fresh = new ActiveState<>(tracker, val);
                                tracker.state = fresh;
                                for (Park<Access<ID, VAL>, Event> pa : tracker.pendingAccess) {
                                    pa.finish(fresh.provision(pa.luggage));
                                }
                                tracker.pendingAccess.clear();
                            } else {
                                tracker.state = null;
                                tracker.lc.trackers.remove(tracker.id);
                            }
                        }
                    };
                }

                tracker.state = null;
                tracker.lc.trackers.remove(tracker.id);
                return () -> {};
            }
        }

        static class SavingState implements State {}

        static class DeletingState implements State {
            boolean hasRunner = false;
        }

        // ---- Park / Parker ----

        static class Park<T, C> {
            enum Signal { FINISH, RESUME, SHUTDOWN }
            final Event signal = new Event();
            Signal signalType;
            T value;
            C luggage;
            void finish(T v) { value = v; signalType = Signal.FINISH; signal.set(); }
            void resume() { signalType = Signal.RESUME; signal.set(); }
            void shutdown() { signalType = Signal.SHUTDOWN; signal.set(); }
        }

        static class ParkException extends RuntimeException {}

        class Parker<T, C> {
            Park<T, C> park;
            ParkException park(Park<T, C> park) { this.park = park; return new ParkException(); }
        }

        <T, C> Supplier<T> run(Function<Parker<T, C>, Supplier<T>> setup) {
            Parker<T, C> parker = new Parker<>();
            Supplier<T> work;
            synchronized (lc.lock) {
                try {
                    work = setup.apply(parker);
                } catch (ParkException e) {
                    return () -> awaitAndRetry(parker, setup);
                }
            }
            Supplier<T> w = work;
            return () -> {
                try { return w.get(); }
                catch (ParkException e) { return awaitAndRetry(parker, setup); }
            };
        }

        private <T, C> T awaitAndRetry(Parker<T, C> parker, Function<Parker<T, C>, Supplier<T>> setup) {
            parker.park.signal.await();
            return switch (parker.park.signalType) {
                case FINISH -> parker.park.value;
                case RESUME -> run(setup).get();
                case SHUTDOWN -> throw new ShuttingDownException();
            };
        }

        // ---- Access ----

        Supplier<Access<ID, VAL>> access(Event onCancel) {
            return this.<Access<ID, VAL>, Event>run((Parker<Access<ID, VAL>, Event> p) -> {
                synchronized (lc.lock) {
                    if (state == null) {
                        LoadingState ls = new LoadingState();
                        ls.hasRunner = true;
                        state = ls;
                        return loadWork(onCancel, p);
                    }
                    if (state instanceof LoadingState ls && !ls.hasRunner) {
                        ls.hasRunner = true;
                        return loadWork(onCancel, p);
                    }
                    if (state instanceof ActiveState active && !active.cancelling) {
                        @SuppressWarnings("unchecked")
                        Access<ID, VAL> a = ((ActiveState<ID, VAL>) active).provision(onCancel);
                        return () -> a;
                    }
                    Park<Access<ID, VAL>, Event> park = new Park<>();
                    park.luggage = onCancel;
                    pendingAccess.add(park);
                    throw p.park(park);
                }
            });
        }

        private Supplier<Access<ID, VAL>> loadWork(Event onCancel, Parker<Access<ID, VAL>, Event> p) {
            return () -> {
                VAL val = null;
                RuntimeException ex = null;
                try { val = lc.core.load(id); }
                catch (RuntimeException e) { ex = e; }

                synchronized (lc.lock) {
                    if (ex != null) {
                        if (!pendingDelete.isEmpty()) {
                            startDeleting();
                            pop(pendingDelete).resume();
                        } else if (!pendingAccess.isEmpty()) {
                            state = new LoadingState();
                            pop(pendingAccess).resume();
                        } else {
                            state = null;
                            lc.trackers.remove(id);
                        }
                        throw ex;
                    }

                    if (!pendingDelete.isEmpty()) {
                        Park<Access<ID, VAL>, Event> park = new Park<>();
                        park.luggage = onCancel;
                        pendingAccess.add(park);
                        startDeleting();
                        pop(pendingDelete).resume();
                        throw p.park(park);
                    }

                    ActiveState<ID, VAL> active = new ActiveState<>(this, val);
                    state = active;
                    for (Park<Access<ID, VAL>, Event> pa : pendingAccess) {
                        pa.finish(active.provision(pa.luggage));
                    }
                    pendingAccess.clear();
                    return active.provision(onCancel);
                }
            };
        }

        // ---- Delete ----

        Runnable delete() {
            Supplier<Void> s = this.<Void, Void>run((Parker<Void, Void> p) -> {
                synchronized (lc.lock) {
                    if (state == null) {
                        DeletingState ds = new DeletingState();
                        ds.hasRunner = true;
                        state = ds;
                        return deleteWork();
                    }
                    if (state instanceof DeletingState ds && !ds.hasRunner) {
                        ds.hasRunner = true;
                        return deleteWork();
                    }
                    if (state instanceof ActiveState) {
                        @SuppressWarnings("unchecked")
                        ActiveState<ID, VAL> active = (ActiveState<ID, VAL>) state;
                        if (!active.cancelling) {
                            active.cancelling = true;
                            for (Event e : active.accessors) e.set();
                        }
                    }
                    Park<Void, Void> park = new Park<>();
                    pendingDelete.add(park);
                    throw p.park(park);
                }
            });
            return () -> s.get();
        }

        private Supplier<Void> deleteWork() {
            return () -> {
                RuntimeException ex = null;
                try { lc.core.delete(id); }
                catch (RuntimeException e) { ex = e; }

                synchronized (lc.lock) {
                    if (!pendingDelete.isEmpty()) {
                        startDeleting();
                        pop(pendingDelete).resume();
                    } else if (!pendingAccess.isEmpty()) {
                        state = new LoadingState();
                        pop(pendingAccess).resume();
                    } else {
                        state = null;
                        lc.trackers.remove(id);
                    }
                }

                if (ex != null) throw ex;
                return null;
            };
        }

        private void startDeleting() {
            state = new DeletingState();
        }

        // ---- Shutdown ----

        void shutdown() {
            for (Park<Access<ID, VAL>, Event> pa : pendingAccess) pa.shutdown();
            pendingAccess.clear();
            for (Park<Void, Void> pd : pendingDelete) pd.shutdown();
            pendingDelete.clear();
            if (state instanceof ActiveState) {
                @SuppressWarnings("unchecked")
                ActiveState<ID, VAL> active = (ActiveState<ID, VAL>) state;
                for (Event e : active.accessors) e.set();
            }
        }
    }
}
