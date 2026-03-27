package keywhale.bukkit.util.loader;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.logging.Level;

import org.bukkit.plugin.java.JavaPlugin;
import org.bukkit.scheduler.BukkitTask;
import org.jspecify.annotations.Nullable;

import keywhale.bukkit.util.loader.op.AccessOperation;
import keywhale.bukkit.util.loader.op.DeleteOperation;
import keywhale.bukkit.util.loader.op.OperationException;
import keywhale.bukkit.util.loader.op.OperationExceptionHandler;
import keywhale.bukkit.util.loader.op.SaveOperation;

public abstract class Loader<ID, VAL> {

    private final JavaPlugin plugin;
    private final Object lock = new Object();
    private final Map<ID, StateTracker<ID, VAL>> trackers = new HashMap<>();
    private final Set<Runnable> pendingRunnables = new HashSet<>();
    private final Queue<Runnable> pendingTopLevelRunnables = new ArrayDeque<>();
    private boolean isShutdown = false;
    private boolean isExpediting = false;

    public Loader(JavaPlugin plugin) {
        this.plugin = plugin;
    }

    private void runAsync(Runnable r) {
        synchronized (this.lock) {
            this.pendingRunnables.add(r);
            if (this.isExpediting) return;

            if (this.plugin.isEnabled()) {
                this.plugin.getServer().getScheduler().runTaskAsynchronously(this.plugin, () -> {
                    boolean shouldRun;
                    synchronized (this.lock) {
                        shouldRun = this.pendingRunnables.remove(r);
                    }

                    if (shouldRun) {
                        r.run();
                    }
                });
            }
        }
    }

    private void runTopLevel(Runnable r) {
        synchronized (this.lock) {
            this.pendingTopLevelRunnables.add(r);
            // Reentrant
            this.runSync(() -> {
                Runnable topLevelRunnable;
                synchronized (this.lock) {
                    topLevelRunnable = this.pendingTopLevelRunnables.poll();
                }
                topLevelRunnable.run();
            });
        }
    }

    private void runSync(Runnable r) {
        synchronized (this.lock) {
            this.pendingRunnables.add(r);
            if (this.isExpediting) return;

            if (this.plugin.isEnabled()) {
                this.plugin.getServer().getScheduler().runTask(this.plugin, () -> {
                    boolean shouldRun;
                    synchronized (this.lock) {
                        shouldRun = this.pendingRunnables.remove(r);
                    }

                    if (shouldRun) {
                        r.run();
                    }
                });
            }
        }
    }

    public static class ShuttingDownException extends IllegalStateException {}

    private void checkShutdown() {
        if (this.isShutdown) {
            throw new ShuttingDownException();
        }
    }

    protected OperationExceptionHandler<ID, VAL> onException() {
        return new OperationExceptionHandler<>() {
            @Override
            public void handleDefault(Exception exc) {
                Loader.this.plugin.getLogger().log(Level.SEVERE, "Operation failed", exc);
            }
        };
    }

    public void access(
        @Nullable ID identifier,
        Accessor<ID, VAL> accessor
    ) {
        synchronized (this.lock) {
            this.checkShutdown();

            this.runTopLevel(() -> {
                synchronized (this.lock) {
                    this.access0(identifier, accessor);
                }
            });
        }
    }

    public void shutdownAndExpedite() {
        synchronized (this.lock) {
            if (this.isShutdown) {
                return;
            }

            this.isShutdown = true;
            this.isExpediting = true;

            Map<ID, StateTracker<ID, VAL>> trackersCopy = new HashMap<>(this.trackers);

            for (StateTracker<ID, VAL> tracker : trackersCopy.values()) {
                tracker.shutdown();
            }

            RuntimeExceptionRoller roller = new RuntimeExceptionRoller();

            while (!this.pendingRunnables.isEmpty()) {
                var pendingRunnablesCopy = new HashSet<>(this.pendingRunnables);
                this.pendingRunnables.clear();

                for (var pr : pendingRunnablesCopy) {
                    roller.exec(pr);
                }
            }

            this.isExpediting = false;
            roller.raise();
        }
    }

    public void delete(ID identifier) {
        synchronized (this.lock) {
            this.checkShutdown();

            this.runTopLevel(() -> {
                synchronized (this.lock) {
                    StateTracker<ID, VAL> tracker = this.trackers.get(identifier);

                    if (tracker == null) {
                        this.deleteReplaceTracker(identifier, new ArrayList<>());
                    } else {
                        tracker.delete();
                    }
                }
            });
        }
    }

    // Under Lock
    private void deleteReplaceTracker(ID identifier, Collection<Accessor<ID, VAL>> accessors) {
        DeletingStateTracker deleteTracker = new DeletingStateTracker(identifier);
        this.trackers.put(identifier, deleteTracker);

        deleteTracker.pendingAccess.addAll(accessors);

        this.runAsync(() -> {
            DeleteOperation op = this.opDelete(identifier);
            try {
                op.run();
            } catch (OperationException e) {
                Loader.this.onException().handleDelete(e, identifier);
            } catch (RuntimeException e) {
                Loader.this.onException().handleDelete(e, identifier);
            } finally {
                this.runSync(() -> {
                    synchronized (this.lock) {
                        deleteTracker.onComplete();
                    }
                });
            }
        });
    }

    // Under Lock
    private void access0(
        @Nullable ID identifier,
        Accessor<ID, VAL> accessor
    ) {
        StateTracker<ID, VAL> tracker = this.trackers.get(identifier);

        if (tracker == null) {
            this.accessOnUnknownState(identifier, accessor);
        } else {
            tracker.access(accessor);
        }
    }

    // Under Lock
    // Only called while active
    private void unloadOnSyncThread(ID identifier, VAL value) {
        UnloadingStateTracker tracker = new UnloadingStateTracker(identifier, value);
        this.trackers.put(identifier, tracker);

        this.unloadAfterTracker(identifier, value, tracker);
    }

    // Under Lock
    private void unloadFromAnyThread(ID identifier, VAL value) {
        UnloadingStateTracker tracker = new UnloadingStateTracker(identifier, value);
        this.trackers.put(identifier, tracker);

        if (this.plugin.getServer().isPrimaryThread()) {
            this.unloadAfterTracker(identifier, value, tracker);
        } else {
            this.runSync(() -> this.unloadAfterTracker(identifier, value, tracker));
        }
    }

    private void unloadAfterTracker(ID identifier, VAL value, UnloadingStateTracker tracker) {
        AtomicBoolean failedPart1 = new AtomicBoolean();

        SaveOperation op = this.opSave(identifier, value);
        try {
            op.runPart1();
        } catch (OperationException e) {
            Loader.this.onException().handleSavePart1(e, identifier, value);
            failedPart1.set(true);
        } catch (RuntimeException e) {
            Loader.this.onException().handleSavePart1(e, identifier, value);
            failedPart1.set(true);
        }

        this.runAsync(() -> {
            if (!failedPart1.get()) {
                try {
                    op.runPart2();
                } catch (OperationException e) {
                    Loader.this.onException().handleSavePart2(e, identifier, value);
                } catch (RuntimeException e) {
                    Loader.this.onException().handleSavePart2(e, identifier, value);
                }
            }

            this.runSync(() -> {
                synchronized (this.lock) {
                    tracker.onComplete();
                }
            });
        });
    }

    // Under Lock
    private void accessAfterDelete(
        ID identifier,
        Collection<Accessor<ID, VAL>> accessors
    ) {
        List<Accessor<ID, VAL>> accessorList = new ArrayList<>(accessors);

        if (accessorList.isEmpty()) {
            return;
        }

        this.runAsync(() -> {
            AccessOperation<ID, VAL> op = this.opAccess(identifier);
            boolean foundOrCreated;
            try {
                foundOrCreated = op.runPart1();
            } catch (OperationException e) {
                Loader.this.onException().handleAccessPart1(e);
                return;
            } catch (RuntimeException e) {
                Loader.this.onException().handleAccessPart1(e);
                return;
            }

            if (foundOrCreated) {
                this.runSync(() -> {
                    synchronized (this.lock) {
                        StateTracker<ID, VAL> tracker = this.trackers.get(op.id());

                        if (tracker == null) {
                            // Make active
                            try {
                                op.runPart2();
                            } catch (OperationException e) {
                                Loader.this.onException().handleAccessPart2(e, op.id());
                                return;
                            } catch (RuntimeException e) {
                                Loader.this.onException().handleAccessPart2(e, op.id());
                                return;
                            }

                            ActiveStateTracker activeTracker
                                = new ActiveStateTracker(op.id(), op.value());

                            var roller = new RuntimeExceptionRoller();

                            for (var accessor : accessorList) {
                                roller.exec(() -> activeTracker.provisionAccess(accessor));
                            }

                            boolean doneDuringInit = activeTracker.accessors.isEmpty();

                            if (doneDuringInit) {
                                if (activeTracker.anyRequiresSave) {
                                    Loader.this.unloadOnSyncThread(op.id(), op.value());
                                }
                            } else {
                                Loader.this.trackers.put(op.id(), activeTracker);
                            }

                            roller.raise();
                        } else {
                            var roller = new RuntimeExceptionRoller();

                            for (var accessor : accessorList) {
                                roller.exec(() -> tracker.access(accessor));
                            }

                            roller.raise();
                        }
                    }

                });
            } else {
                this.runSync(() -> {
                    var roller = new RuntimeExceptionRoller();

                    for (var accessor : accessorList) {
                        roller.exec(accessor::onNotFound);
                    }

                    roller.raise();
                });
            }
        });
    }

    // Under Lock
    private void accessOnUnknownState(
        @Nullable ID identifier,
        Accessor<ID, VAL> accessor
    ) {
        this.runAsync(() -> {
            AccessOperation<ID, VAL> op = this.opAccess(identifier);
            boolean foundOrCreated;
            try {
                foundOrCreated = op.runPart1();
            } catch (OperationException e) {
                Loader.this.onException().handleAccessPart1(e);
                return;
            } catch (RuntimeException e) {
                Loader.this.onException().handleAccessPart1(e);
                return;
            }

            if (foundOrCreated) {
                this.runSync(() -> {
                    synchronized (this.lock) {
                        StateTracker<ID, VAL> tracker = this.trackers.get(op.id());

                        if (tracker == null) {
                            // Make active
                            try {
                                op.runPart2();
                            } catch (OperationException e) {
                                Loader.this.onException().handleAccessPart2(e, op.id());
                                return;
                            } catch (RuntimeException e) {
                                Loader.this.onException().handleAccessPart2(e, op.id());
                                return;
                            }

                            ActiveStateTracker activeTracker
                                = new ActiveStateTracker(op.id(), op.value());

                            boolean doneDuringInit = activeTracker.provisionAccess(accessor);

                            if (doneDuringInit) {
                                if (activeTracker.anyRequiresSave) {
                                    Loader.this.unloadOnSyncThread(op.id(), op.value());
                                }
                            } else {
                                Loader.this.trackers.put(op.id(), activeTracker);
                            }
                        } else {
                            tracker.access(accessor);
                        }
                    }

                });
            } else {
                this.runSync(accessor::onNotFound);
            }
        });
    }

    private static class RuntimeExceptionRoller {
        private RuntimeException top;

        public void add(RuntimeException runtimeException) {
            if (this.top == null) {
                this.top = runtimeException;
            } else {
                this.top.addSuppressed(runtimeException);
            }
        }

        public void raise() {
            if (this.top != null) {
                throw this.top;
            }
        }

        public void exec(Runnable r) {
            try {
                r.run();
            } catch (RuntimeException re) {
                this.add(re);
            }
        }
    }

    private interface StateTracker<ID, VAL> {
        void access(Accessor<ID, VAL> accessor);
        void delete();
        void shutdown();
    }

    private class ActiveStateTracker implements StateTracker<ID, VAL> {

        private final ActiveStateTracker athis = this;

        private final ID identifier;
        private final VAL value;

        private Substate<ID, VAL> substate = new ActiveSubstate();
        private boolean anyRequiresSave = false;

        private final Set<Accessor<ID, VAL>> accessors = new HashSet<>();

        private static interface Substate<ID, VAL> {
            public void access(Accessor<ID, VAL> accessor);
            void delete();
            void shutdown();
            void done(Accessor<ID, VAL> accessor);
        }

        private class ActiveSubstate implements Substate<ID, VAL> {

            @Override
            public void access(Accessor<ID, VAL> accessor) {
                athis.provisionAccess(accessor);
            }

            @Override
            public void delete() {
                athis.substate = new DeletingSubstate();
                this.cancel();
            }

            @Override
            public void shutdown() {
                athis.substate = new ShutdownSubstate();
                this.cancel();
            }

            private void cancel() {
                RuntimeExceptionRoller roller = new RuntimeExceptionRoller();

                for (var a : new ArrayList<>(athis.accessors)) {
                    roller.exec(a::cancel);
                }

                roller.raise();
            }

            @Override
            public void done(Accessor<ID, VAL> accessor) {
                athis.accessors.remove(accessor);

                if (athis.accessors.isEmpty()) {
                    if (athis.anyRequiresSave) {
                        Loader.this.unloadFromAnyThread(athis.identifier, athis.value);
                    } else {
                        Loader.this.trackers.remove(athis.identifier);
                    }
                }
            }

        }

        private class DeletingSubstate implements Substate<ID, VAL> {

            private final List<Accessor<ID, VAL>> pendingAccess = new ArrayList<>();

            @Override
            public void access(Accessor<ID, VAL> accessor) {
                this.pendingAccess.add(accessor);
            }

            @Override
            public void delete() {}

            @Override
            public void shutdown() {
                athis.substate = new ShutdownSubstate();
            }

            @Override
            public void done(Accessor<ID, VAL> accessor) {
                athis.accessors.remove(accessor);

                if (athis.accessors.isEmpty()) {
                    Loader.this.deleteReplaceTracker(athis.identifier, this.pendingAccess);
                }
            }

        }

        private class ShutdownSubstate implements Substate<ID, VAL> {

            @Override
            public void access(Accessor<ID, VAL> accessor) {}

            @Override
            public void delete() {}

            @Override
            public void shutdown() {}

            @Override
            public void done(Accessor<ID, VAL> accessor) {
                athis.accessors.remove(accessor);

                if (athis.accessors.isEmpty()) {
                    if (athis.anyRequiresSave) {
                        Loader.this.unloadFromAnyThread(athis.identifier, athis.value);
                    } else {
                        Loader.this.trackers.remove(athis.identifier);
                    }
                }
            }

        }

        ActiveStateTracker(ID identifier, VAL value) {
            this.identifier = identifier;
            this.value = value;
        }

        @Override
        public void access(Accessor<ID, VAL> accessor) {
            this.substate.access(accessor);
        }

        private boolean provisionAccess(Accessor<ID, VAL> accessor) {
            AtomicBoolean isDone = new AtomicBoolean();
            AtomicBoolean initSuccess = new AtomicBoolean();
            AtomicBoolean doneDuringInit = new AtomicBoolean();
            AtomicBoolean isInit = new AtomicBoolean();

            final Object doneLock = new Object();

            Access<ID, VAL> access = new Access<>() {

                @Override
                public VAL value() {
                    return athis.value;
                }

                @Override
                public ID id() {
                    return athis.identifier;
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
                                athis.substate.done(accessor);
                            }
                        }
                    }
                }
            };

            isInit.set(true);
            try {
                accessor.init(access);
                initSuccess.set(true);
            } finally {
                isInit.set(false);
            }

            if (!doneDuringInit.get()) {
                this.accessors.add(accessor);
            }

            if (accessor.requiresSave()) {
                this.anyRequiresSave = true;
            }

            return doneDuringInit.get();
        }

        @Override
        public void delete() {
            this.substate.delete();
        }

        @Override
        public void shutdown() {
            this.substate.shutdown();
        }

    }

    private class UnloadingStateTracker implements StateTracker<ID, VAL> {

        private final ID identifier;
        private final VAL value;

        private final List<Accessor<ID, VAL>> pendingAccess = new ArrayList<>();

        private boolean pendingDelete = false;
        private boolean pendingShutdown = false;

        UnloadingStateTracker(ID identifier, VAL value) {
            this.identifier = identifier;
            this.value = value;
        }

        @Override
        public void access(Accessor<ID, VAL> accessor) {
            this.pendingAccess.add(accessor);
        }

        // Under Lock
        public void onComplete() {
            Loader.this.trackers.remove(this.identifier);

            if (this.pendingShutdown) {
                this.pendingAccess.clear();
                this.pendingDelete = false;
            } else if (this.pendingDelete) {
                Loader.this.deleteReplaceTracker(this.identifier, this.pendingAccess);
            } else if (!this.pendingAccess.isEmpty()) {
                ActiveStateTracker activeTracker
                    = new ActiveStateTracker(this.identifier, this.value);

                var roller = new RuntimeExceptionRoller();

                for (var accessor : this.pendingAccess) {
                    roller.exec(() -> activeTracker.provisionAccess(accessor));
                }

                boolean doneDuringInit = activeTracker.accessors.isEmpty();

                if (doneDuringInit) {
                    if (activeTracker.anyRequiresSave) {
                        Loader.this.unloadOnSyncThread(this.identifier, this.value);
                    }
                } else {
                    Loader.this.trackers.put(this.identifier, activeTracker);
                }

                roller.raise();
            }
        }

        @Override
        public void delete() {
            this.pendingDelete = true;
        }

        @Override
        public void shutdown() {
            this.pendingShutdown = true;
        }

    }

    private class DeletingStateTracker implements StateTracker<ID, VAL> {

        private final ID identifier;

        private final List<Accessor<ID, VAL>> pendingAccess = new ArrayList<>();

        private boolean pendingShutdown = false;

        public DeletingStateTracker(ID identifier) {
            this.identifier = identifier;
        }

        @Override
        public void access(Accessor<ID, VAL> accessor) {
            this.pendingAccess.add(accessor);
        }

        @Override
        public void delete() {}

        @Override
        public void shutdown() {
            this.pendingShutdown = true;
        }

        public void onComplete() {
            Loader.this.trackers.remove(this.identifier);

            if (this.pendingShutdown) {
                this.pendingAccess.clear();
                return;
            } else if (!this.pendingAccess.isEmpty()) {
                Loader.this.accessAfterDelete(this.identifier, this.pendingAccess);
            }
        }

    }

    public BukkitTask autoRelease(
        Access<ID, VAL> access,
        Supplier<Boolean> isAccessing,
        long intervalTicks
    ) {
        AtomicReference<BukkitTask> bt = new AtomicReference<>();
        BukkitTask task = this.plugin.getServer().getScheduler().runTaskTimer(this.plugin, () -> {
            if (!isAccessing.get()) {
                bt.get().cancel();
                access.done();
            }
        }, intervalTicks, intervalTicks);
        bt.set(task);
        return task;
    }

    // This should NOT call any methods on Loader
    protected abstract AccessOperation<ID, VAL> opAccess(@Nullable ID identifier);
    // This should NOT call any methods on Loader
    protected abstract SaveOperation opSave(ID identifier, VAL value);
    // This should NOT call any methods on Loader
    protected abstract DeleteOperation opDelete(ID identifier);

}
