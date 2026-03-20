package keywhale.bukkit.util.loader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import org.bukkit.plugin.java.JavaPlugin;
import org.jspecify.annotations.NonNull;
import org.jspecify.annotations.Nullable;

import keywhale.bukkit.util.loader.op.AccessOperation;
import keywhale.bukkit.util.loader.op.DeleteOperation;
import keywhale.bukkit.util.loader.op.SaveOperation;
import keywhale.bukkit.util.loader.op.exc.AccessOperationExceptionHandler;
import keywhale.bukkit.util.loader.op.exc.DeleteOperationException;
import keywhale.bukkit.util.loader.op.exc.DeleteOperationExceptionHandler;
import keywhale.bukkit.util.loader.op.exc.Part1AccessOperationException;
import keywhale.bukkit.util.loader.op.exc.Part1SaveOperationException;
import keywhale.bukkit.util.loader.op.exc.Part2AccessOperationException;
import keywhale.bukkit.util.loader.op.exc.Part2SaveOperationException;
import keywhale.bukkit.util.loader.op.exc.SaveOperationExceptionHandler;

public abstract class Loader<ID, VAL> {

    private final JavaPlugin plugin;
    private final Object lock = new Object();
    private final Map<ID, StateTracker<ID, VAL>> trackers = new HashMap<>();
    private final Set<Runnable> pendingRunnables = new HashSet<>();
    private boolean isShutdown = false;

    public Loader(JavaPlugin plugin) {
        this.plugin = plugin;
    }

    private void runAsync(Runnable r) {
        synchronized (this.lock) {
            this.pendingRunnables.add(r);
        }
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

    private void runSync(Runnable r) {
        synchronized (this.lock) {
            this.pendingRunnables.add(r);
        }
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

    public static class ShuttingDownException extends IllegalStateException {}

    private void checkShutdown() {
        if (this.isShutdown) {
            throw new ShuttingDownException();
        }
    }

    public void access(
        @Nullable ID identifier,
        Accessor<ID, VAL> accessor,
        @Nullable Runnable onNotFound,
        @Nullable AccessOperationExceptionHandler onAccessException,
        @Nullable SaveOperationExceptionHandler onSaveException
    ) {
        synchronized (this.lock) {
            this.checkShutdown();

            this.runSync(() -> {
                synchronized (this.lock) {
                    this.access0(
                        identifier,
                        accessor,
                        onNotFound,
                        this.getExceptionHandler(onAccessException),
                        this.getExceptionHandler(onSaveException)
                    );
                }
            });
        }
    }

    public void shutdown(@Nullable SaveOperationExceptionHandler onSaveException) {
        synchronized (this.lock) {
            if (this.isShutdown) {
                return;
            }

            this.isShutdown = true;

            this.runSync(() -> {
                synchronized (this.lock) {
                    Map<ID, StateTracker<ID, VAL>> trackersCopy
                        = new HashMap<>(this.trackers);
                    
                    for (StateTracker<ID, VAL> tracker : trackersCopy.values()) {
                        tracker.shutdown(this.getExceptionHandler(onSaveException));
                    }
                }
            });
        }
    }

    public void shutdown() {
        this.shutdown(null);
    }

    private DeleteOperationExceptionHandler getExceptionHandler(
        @Nullable DeleteOperationExceptionHandler onException
    ) {
        if (onException == null) {
            return new DeleteOperationExceptionHandler() {

                @Override
                public void handleDefault(Exception exc) {
                    Loader.this.plugin.getLogger().log(Level.SEVERE, "Delete operation failed", exc);
                }

            };
        } else {
            return onException;
        }
    }

    private AccessOperationExceptionHandler getExceptionHandler(
        @Nullable AccessOperationExceptionHandler onException
    ) {
        if (onException == null) {
            return new AccessOperationExceptionHandler() {

                @Override
                public void handleDefault(Exception exc) {
                    Loader.this.plugin.getLogger().log(Level.SEVERE, "Access operation failed", exc);
                }

            };
        } else {
            return onException;
        }
    }

    private SaveOperationExceptionHandler getExceptionHandler(
        @Nullable SaveOperationExceptionHandler onException
    ) {
        if (onException == null) {
            return new SaveOperationExceptionHandler() {

                @Override
                public void handleDefault(Exception exc) {
                    Loader.this.plugin.getLogger().log(Level.SEVERE, "Save operation failed", exc);
                }

            };
        } else {
            return onException;
        }
    }

    public void delete(
        ID identifier, 
        @Nullable DeleteOperationExceptionHandler onException
    ) {
        synchronized (this.lock) {
            this.checkShutdown();

            this.runSync(() -> {
                synchronized (this.lock) {
                    StateTracker<ID, VAL> tracker = this.trackers.get(identifier);

                    if (tracker == null) {
                        this.deleteReplaceTracker(
                            identifier, 
                            this.getExceptionHandler(onException)
                        );
                    } else {
                        tracker.delete(this.getExceptionHandler(onException));
                    }
                }
            });
        }
    }

    public void delete(ID identifier) {
        this.delete(identifier, null);
    }

    // Under Lock
    private void deleteReplaceTracker(
        ID identifier, 
        @NonNull DeleteOperationExceptionHandler onDeleteException
    ) {
        DeletingStateTracker deleteTracker = new DeletingStateTracker();
        this.trackers.put(identifier, deleteTracker);

        this.runAsync(() -> {
            DeleteOperation op = this.opDelete(identifier);
            try {
                op.run();
            } catch (DeleteOperationException e) {
                onDeleteException.handle(e);
            } catch (RuntimeException e) {
                onDeleteException.handle(e);
            } finally {
                this.runSync(() -> {
                    synchronized (this.lock) {
                        this.trackers.remove(identifier);
                    }
                });
            }
        });
    }

    // Under Lock
    private void access0(
        @Nullable ID identifier,
        Accessor<ID, VAL> accessor,
        @Nullable Runnable onNotFound,
        @NonNull AccessOperationExceptionHandler onAccessException,
        @NonNull SaveOperationExceptionHandler onSaveException
    ) {
        StateTracker<ID, VAL> tracker = this.trackers.get(identifier);

        if (tracker == null) {
            this.accessOnUnknownState(
                identifier, 
                accessor, 
                onNotFound,
                onAccessException,
                onSaveException
            );
        } else {
            tracker.access(accessor, onNotFound, onSaveException);
        }
    }

    // Under Lock
    // Only called while active
    private void unload(ID identifier, VAL value, @NonNull SaveOperationExceptionHandler onSaveException) {
        UnloadingStateTracker tracker = new UnloadingStateTracker(identifier, value);
        this.trackers.put(identifier, tracker);

        AtomicBoolean failedPart1 = new AtomicBoolean();

        SaveOperation op = this.opSave(identifier, value);
        try {
            op.runPart1();
        } catch (Part1SaveOperationException e) {
            onSaveException.handlePart1(e);
            failedPart1.set(true);
        } catch (RuntimeException e) {
            onSaveException.handlePart1(e);
            failedPart1.set(true);
        }

        this.runAsync(() -> {
            if (!failedPart1.get()) {
                try {
                    op.runPart2();
                } catch (Part2SaveOperationException e) {
                    onSaveException.handlePart2(e);
                } catch (RuntimeException e) {
                    onSaveException.handlePart2(e);
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
    private void accessOnUnknownState(
        @Nullable ID identifier,
        Accessor<ID, VAL> accessor,
        @Nullable Runnable onNotFound,
        @NonNull AccessOperationExceptionHandler onAccessException,
        @NonNull SaveOperationExceptionHandler onSaveException
    ) {
        this.runAsync(() -> {
            AccessOperation<ID, VAL> op = this.opAccess(identifier);
            boolean foundOrCreated;
            try {
                foundOrCreated = op.runPart1();
            } catch (Part1AccessOperationException e) {
                onAccessException.handlePart1(e);
                return;
            } catch (RuntimeException e) {
                onAccessException.handlePart1(e);
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
                            } catch (Part2AccessOperationException e) {
                                onAccessException.handlePart2(e);
                                return;
                            } catch (RuntimeException e) {
                                onAccessException.handlePart2(e);
                                return;
                            }

                            ActiveStateTracker activeTracker
                                = new ActiveStateTracker(op.id(), op.value());

                            boolean doneDuringInit
                                = activeTracker.provisionAccess(accessor, onSaveException);

                            if (doneDuringInit) {
                                this.unload(op.id(), op.value(), onSaveException);
                            } else {
                                this.trackers.put(op.id(), activeTracker);
                            }
                        } else {
                            tracker.access(accessor, onNotFound, onSaveException);
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
        SaveOperationExceptionHandler onSaveException;
    }

    public void access(
        @Nullable ID identifier,
        Accessor<ID, VAL> accessor
    ) {
        this.access(identifier, accessor, null, null, null);
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

    private static class CompositeSaveOperationExceptionHandler implements SaveOperationExceptionHandler {

        private final List<SaveOperationExceptionHandler> handlers;

        CompositeSaveOperationExceptionHandler(List<SaveOperationExceptionHandler> handlers) {
            this.handlers = handlers;
        }

        @Override
        public void handlePart1(Part1SaveOperationException exc) {
            var roller = new RuntimeExceptionRoller();
            for (var handler : this.handlers) {
                roller.exec(() -> handler.handlePart1(exc));
            }
            roller.raise();
        }

        @Override
        public void handlePart1(RuntimeException exc) {
            var roller = new RuntimeExceptionRoller();
            for (var handler : this.handlers) {
                roller.exec(() -> handler.handlePart1(exc));
            }
            roller.raise();
        }

        @Override
        public void handlePart2(Part2SaveOperationException exc) {
            var roller = new RuntimeExceptionRoller();
            for (var handler : this.handlers) {
                roller.exec(() -> handler.handlePart2(exc));
            }
            roller.raise();
        }

        @Override
        public void handlePart2(RuntimeException exc) {
            var roller = new RuntimeExceptionRoller();
            for (var handler : this.handlers) {
                roller.exec(() -> handler.handlePart2(exc));
            }
            roller.raise();
        }

    }

    private interface StateTracker<ID, VAL> {
        void access(
            Accessor<ID, VAL> accessor, 
            @Nullable Runnable onNotFound,
            SaveOperationExceptionHandler onSaveException
        );
        void delete(
            @NonNull DeleteOperationExceptionHandler onDeleteException
        );
        void shutdown(@NonNull SaveOperationExceptionHandler onSaveException);
    }

    private class ActiveStateTracker implements StateTracker<ID, VAL> {

        private final ID identifier;
        private final VAL value;

        private final Set<Accessor<ID, VAL>> accessors = new HashSet<>();
            
        private boolean isShuttingDown = false;

        ActiveStateTracker(ID identifier, VAL value) {
            this.identifier = identifier;
            this.value = value;
        }

        @Override
        public void access(
            Accessor<ID, VAL> accessor, 
            @Nullable Runnable onNotFound,
            SaveOperationExceptionHandler onSaveException
        ) {
            this.provisionAccess(accessor, onSaveException);
        }

        public void loadPending(
            List<PendingAccessRequest> pars
        ) {
            var roller = new RuntimeExceptionRoller();
            for (var par : pars) {
                roller.exec(() -> this.provisionAccess(par.accessor, par.onSaveException));
            }

            try {
                roller.raise();
            } finally {
                if (this.accessors.isEmpty()) {
                    Loader.this.unload(this.identifier, this.value, new CompositeSaveOperationExceptionHandler(
                        pars.stream().map(par -> par.onSaveException).toList()
                    ));
                }
            }
        }

        private boolean isActive() {
            return ((Loader.this.trackers.get(this.identifier) == this) && !this.isShuttingDown);
        }

        private boolean provisionAccess(
            Accessor<ID, VAL> accessor, 
            @NonNull SaveOperationExceptionHandler onSaveException
        ) {
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
                                        Loader.this.unload(ast.identifier, ast.value, onSaveException);
                                    }
                                }
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

            return doneDuringInit.get();
        }

        @Override
        public void delete(
            @NonNull DeleteOperationExceptionHandler onDeleteException
        ) {
            Loader.this.deleteReplaceTracker(this.identifier, onDeleteException);

            try {
                var roller = new RuntimeExceptionRoller();
                for (var accessor : this.accessors) {
                    roller.exec(accessor::cancel);
                }
                roller.raise();
            } finally {
                this.accessors.clear();
            }
        }

        @Override
        public void shutdown(@NonNull SaveOperationExceptionHandler onSaveException) {
            this.isShuttingDown = true;

            try {
                var roller = new RuntimeExceptionRoller();
                for (var accessor : this.accessors) {
                    roller.exec(accessor::cancel);
                }
                roller.raise();
            } finally {
                this.accessors.clear();
                Loader.this.unload(this.identifier, this.value, onSaveException);
            }
        }

    }

    private class UnloadingStateTracker implements StateTracker<ID, VAL> {

        private final ID identifier;
        private final VAL value;

        private final List<PendingAccessRequest> pendingAccess = new ArrayList<>();

        private boolean pendingDelete = false;
        private boolean pendingShutdown = false;

        private DeleteOperationExceptionHandler pendingDeleteExceptionHandler;

        UnloadingStateTracker(ID identifier, VAL value) {
            this.identifier = identifier;
            this.value = value;
        }

        @Override
        public void access(
            Accessor<ID, VAL> accessor, 
            @Nullable Runnable onNotFound,
            SaveOperationExceptionHandler onSaveException
        ) {
            var par = new PendingAccessRequest(); {
                par.accessor = accessor;
                par.onNotFound = onNotFound;
                par.onSaveException = onSaveException;
            }

            this.pendingAccess.add(par);
        }

        public void onComplete() {
            Loader.this.trackers.remove(this.identifier);

            if (this.pendingShutdown) {
                this.pendingAccess.clear();
                this.pendingDelete = false;
            } else if (this.pendingDelete) {
                try {
                    var roller = new RuntimeExceptionRoller();

                    for (var par : this.pendingAccess) {
                        if (par.onNotFound != null) {
                            roller.exec(par.onNotFound);
                        }
                    }

                    roller.raise();
                } finally {
                    Loader.this.deleteReplaceTracker(this.identifier, this.pendingDeleteExceptionHandler);
                }
            } else if (!this.pendingAccess.isEmpty()) {
                ActiveStateTracker activeTracker
                    = new ActiveStateTracker(this.identifier, this.value);
                Loader.this.trackers.put(this.identifier, activeTracker);

                activeTracker.loadPending(this.pendingAccess);
            }
        }

        @Override
        public void delete(@NonNull DeleteOperationExceptionHandler onDeleteException) {
            this.pendingDelete = true;
            this.pendingDeleteExceptionHandler = onDeleteException;
        }

        @Override
        public void shutdown(@NonNull SaveOperationExceptionHandler onSaveException) {
            this.pendingShutdown = true;
        }

    }

    private class DeletingStateTracker implements StateTracker<ID, VAL> {

        @Override
        public void access(
            Accessor<ID, VAL> accessor, 
            @Nullable Runnable onNotFound, 
            SaveOperationExceptionHandler onSaveException
        ) {
            // Into the void...
        }

        @Override
        public void delete(@NonNull DeleteOperationExceptionHandler onDeleteException) {
            // Into the void...
        }

        @Override
        public void shutdown(@NonNull SaveOperationExceptionHandler onSaveException) {
            // Into the void...
        }

    }

    // This should NOT call any methods on Loader
    protected abstract AccessOperation<ID, VAL> opAccess(@Nullable ID identifier);
    // This should NOT call any methods on Loader
    protected abstract SaveOperation opSave(ID identifier, VAL value);
    // This should NOT call any methods on Loader
    protected abstract DeleteOperation opDelete(ID identifier);

    public void expedite() {
        synchronized (this.lock) {
            RuntimeExceptionRoller roller = new RuntimeExceptionRoller();

            while (!this.pendingRunnables.isEmpty()) {
                var pendingRunnablesCopy = new HashSet<>(this.pendingRunnables);
                this.pendingRunnables.clear();

                for (var pr : pendingRunnablesCopy) {
                    roller.exec(pr);
                }
            }
            
            roller.raise();
        }
    }

    public void shutdownNow() {
        this.shutdown();
        this.expedite();
    }
    
}
