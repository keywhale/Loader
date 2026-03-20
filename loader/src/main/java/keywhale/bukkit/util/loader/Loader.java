package keywhale.bukkit.util.loader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bukkit.plugin.java.JavaPlugin;
import org.jspecify.annotations.Nullable;

import keywhale.bukkit.util.loader.op.AccessOperation;
import keywhale.bukkit.util.loader.op.DeleteOperation;
import keywhale.bukkit.util.loader.op.SaveOperation;

public abstract class Loader<ID, VAL> {

    private final JavaPlugin plugin;
    
    private final Object lock = new Object();

    private final Map<ID, StateTracker<ID, VAL>> trackers = new HashMap<>();

    private final Set<Thread> illegalThreads = new HashSet<>();
    
    public Loader(JavaPlugin plugin) {
        this.plugin = plugin;
    }

    private void runAsync(Runnable r) {
        this.plugin.getServer().getScheduler().runTaskAsynchronously(this.plugin, () -> {
            r.run();
        });
    }

    private void runSync(Runnable r) {
        if (
            this.plugin.getServer().isPrimaryThread() 
            && !this.illegalThreads.contains(Thread.currentThread())
        ) {
            r.run(); // handle exception?
        } else {
            this.plugin.getServer().getScheduler().runTask(this.plugin, () -> {
                r.run();
            });
        }
    }

    public void access(
        @Nullable ID identifier,
        Accessor<ID, VAL> accessor,
        @Nullable Runnable onNotFound
    ) {
        this.runSync(() -> {
            synchronized (this.lock) {
                this.access0(identifier, accessor, onNotFound);
            }
        });
    }

    public void delete(ID identifier) {
        this.runSync(() -> {
            synchronized (this.lock) {
                StateTracker<ID, VAL> tracker = this.trackers.get(identifier);

                if (tracker == null) {
                    this.deleteReplaceTracker(identifier);
                } else {
                    tracker.delete();
                }
            }
        });
    }

    // Under Lock
    private void deleteReplaceTracker(ID identifier) {
        DeletingStateTracker deleteTracker = new DeletingStateTracker();
        this.trackers.put(identifier, deleteTracker);

        this.runAsync(() -> {
            DeleteOperation op = this.opDelete(identifier);
            op.run();

            this.runSync(() -> {
                synchronized (this.lock) {
                    this.trackers.remove(identifier);
                }
            });
        });
    }

    // Under Lock
    private void access0(
        @Nullable ID identifier,
        Accessor<ID, VAL> accessor,
        @Nullable Runnable onNotFound
    ) {
        StateTracker<ID, VAL> tracker = this.trackers.get(identifier);

        if (tracker == null) {
            this.accessOnUnknownState(identifier, accessor, onNotFound);
        } else {
            tracker.access(accessor, onNotFound);
        }
    }

    // Under Lock
    // Only called while active
    private void unload(ID identifier, VAL value) {
        UnloadingStateTracker tracker = new UnloadingStateTracker(identifier, value);
        this.trackers.put(identifier, tracker);

        SaveOperation op = this.opSave(identifier, value);
        op.runPart1(); // handle exception?

        this.runAsync(() -> {
            op.runPart2(); // handle exception?

            this.runSync(() -> {
                synchronized (this.lock) {
                    tracker.onComplete();
                }
            });
        });
    }

    // Under Lock
    private void accessOnUnknownState(
        @Nullable ID identifier,
        Accessor<ID, VAL> accessor,
        @Nullable Runnable onNotFound
    ) {
        this.runAsync(() -> {
            AccessOperation<ID, VAL> op = this.opAccess(identifier);
            boolean foundOrCreated = op.runPart1(); // handle exception?

            if (foundOrCreated) {
                this.runSync(() -> {
                    synchronized (this.lock) {
                        StateTracker<ID, VAL> tracker = this.trackers.get(op.id());

                        if (tracker == null) {
                            // Make active
                            op.runPart2(); // handle exception?

                            ActiveStateTracker activeTracker
                                = new ActiveStateTracker(op.id(), op.value());

                            boolean doneDuringInit
                                = activeTracker.provisionAccess(accessor); // handle exception?

                            if (doneDuringInit) {
                                this.unload(op.id(), op.value());
                            } else {
                                this.trackers.put(op.id(), activeTracker);
                            }
                        } else {
                            tracker.access(accessor, onNotFound);
                        }
                    }
                    
                });
            } else {
                if (onNotFound != null) {
                    this.runSync(onNotFound);
                }
            }
        });
    }

    private class PendingAccessRequest {
        Accessor<ID, VAL> accessor;
        @Nullable Runnable onNotFound;
    }

    public void access(
        @Nullable ID identifier,
        Accessor<ID, VAL> accessor
    ) {
        this.access(identifier, accessor, null);
    }

    private interface StateTracker<ID, VAL> {
        void access(Accessor<ID, VAL> accessor, @Nullable Runnable onNotFound);
        void delete();
    }

    private class ActiveStateTracker implements StateTracker<ID, VAL> {

        private final ID identifier;
        private final VAL value;

        private final Set<Accessor<ID, VAL>> accessors = new HashSet<>();

        ActiveStateTracker(ID identifier, VAL value) {
            this.identifier = identifier;
            this.value = value;
        }

        @Override
        public void access(Accessor<ID, VAL> accessor, @Nullable Runnable onNotFound) {
            this.provisionAccess(accessor);
        }

        public void loadPending(List<PendingAccessRequest> pars) {
            for (var par : pars) {
                this.provisionAccess(par.accessor);
            }

            if (this.accessors.isEmpty()) {
                Loader.this.unload(this.identifier, this.value);
            }
        }

        private boolean isActive() {
            return (Loader.this.trackers.get(this.identifier) == this);
        }

        private boolean provisionAccess(Accessor<ID, VAL> accessor) {
            AtomicBoolean isDone = new AtomicBoolean();
            AtomicBoolean initSuccess = new AtomicBoolean();
            AtomicBoolean doneDuringInit = new AtomicBoolean();
            AtomicBoolean isInit = new AtomicBoolean();

            final Object doneLock = new Object();
            final var ast = this;

            Access<ID, VAL> access = new Access<>() {

                @Override
                public VAL value() {
                    return ast.value;
                }

                @Override
                public ID id() {
                    return ast.identifier;
                }

                @Override
                public void done() {
                    synchronized (doneLock) {
                        if (isDone.get()) {
                            return;
                        }

                        isDone.set(true);

                        if (!initSuccess.get()) {
                            return;
                        } else if (isInit.get()) {
                            doneDuringInit.set(true);
                        } else {
                            synchronized (Loader.this.lock) {
                                if (ast.isActive()) {
                                    ast.accessors.remove(accessor);

                                    if (ast.accessors.isEmpty()) {
                                        Loader.this.unload(ast.identifier, ast.value);
                                    }
                                }
                            }
                        }
                    }
                }
            };

            isInit.set(true);
            try {
                Loader.this.illegalThreads.add(Thread.currentThread());
                accessor.init(access); // handle exception?
                initSuccess.set(true);
            } finally {
                Loader.this.illegalThreads.remove(Thread.currentThread());
                isInit.set(false);
            }

            if (!doneDuringInit.get()) {
                this.accessors.add(accessor);
            }

            return doneDuringInit.get();
        }

        @Override
        public void delete() {
            Loader.this.deleteReplaceTracker(this.identifier);

            try {
                Loader.this.illegalThreads.add(Thread.currentThread());
                for (var accessor : this.accessors) {
                    accessor.cancel(); // handle exception?
                }
            } finally {
                Loader.this.illegalThreads.remove(Thread.currentThread());
                this.accessors.clear();
            }
        }

    }

    private class UnloadingStateTracker implements StateTracker<ID, VAL> {

        private final ID identifier;
        private final VAL value;

        private final List<PendingAccessRequest> pendingAccess = new ArrayList<>();
        private boolean pendingDelete = false;

        UnloadingStateTracker(ID identifier, VAL value) {
            this.identifier = identifier;
            this.value = value;
        }

        @Override
        public void access(Accessor<ID, VAL> accessor, @Nullable Runnable onNotFound) {
            var par = new PendingAccessRequest(); {
                par.accessor = accessor;
                par.onNotFound = onNotFound;
            }

            this.pendingAccess.add(par);
        }

        public void onComplete() {
            Loader.this.trackers.remove(this.identifier);

            if (this.pendingDelete) {
                try {
                    Loader.this.illegalThreads.add(Thread.currentThread());
                    for (var par : this.pendingAccess) {
                        if (par.onNotFound != null) {
                            par.onNotFound.run();
                        }
                    }
                } finally {
                    Loader.this.illegalThreads.remove(Thread.currentThread());
                    Loader.this.deleteReplaceTracker(this.identifier);
                }
            } else if (!this.pendingAccess.isEmpty()) {
                ActiveStateTracker activeTracker
                    = new ActiveStateTracker(this.identifier, this.value);
                Loader.this.trackers.put(this.identifier, activeTracker);

                activeTracker.loadPending(this.pendingAccess);
            }
        }

        @Override
        public void delete() {
            this.pendingDelete = true;
        }

    }

    private class DeletingStateTracker implements StateTracker<ID, VAL> {

        @Override
        public void access(Accessor<ID, VAL> accessor, @Nullable Runnable onNotFound) {
            // Into the void...
        }

        @Override
        public void delete() {
            // Into the void...
        }

    }

    // This should NOT call any methods on Loader
    protected abstract AccessOperation<ID, VAL> opAccess(@Nullable ID identifier);
    // This should NOT call any methods on Loader
    protected abstract SaveOperation opSave(ID identifier, VAL value);
    // This should NOT call any methods on Loader
    protected abstract DeleteOperation opDelete(ID identifier);

    // TODO: Implement Shutdown
    // TODO: Implement Expediting
    
}
