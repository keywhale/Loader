package keywhale.util.loader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import keywhale.util.loader.op.AccessOperation;
import keywhale.util.loader.op.DeleteOperation;
import keywhale.util.loader.op.SaveOperation;

public abstract class Loader<ID, VAL> {

    private final Object lock = new Object();
    private final Map<ID, StateTracker<ID, VAL>> trackers = new HashMap<>();
    private boolean isShutdown = false;

    public static class ShuttingDownException extends IllegalStateException {}

    private void checkShutdown() {
        if (this.isShutdown) {
            throw new ShuttingDownException();
        }
    }

    public void shutdown() {
        synchronized (this.lock) {
            if (this.isShutdown) {
                return;
            }

            this.isShutdown = true;

            for (StateTracker<ID, VAL> tracker : new ArrayList<>(this.trackers.values())) {
                tracker.shutdown();
            }
        }
    }

    // Under Lock
    private void deleteReplaceTracker(ID identifier, DeleteOperation op, Collection<AccessRequest<ID, VAL>> accessors) {
        DeletingStateTracker deleteTracker = new DeletingStateTracker(identifier);
        this.trackers.put(identifier, deleteTracker);

        deleteTracker.pendingAccess.addAll(accessors);

        op.start(() -> {
            synchronized (this.lock) {
                deleteTracker.onComplete();
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

    private record AccessRequest<ID, VAL>(
        AccessOperation<ID, VAL> op,
        Accessor<ID, VAL> accessor
    ) {}

    private interface StateTracker<ID, VAL> {
        void access(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor);
        void delete(DeleteOperation op);
        void shutdown();
    }

    private class LoadingStateTracker implements StateTracker<ID, VAL> {

        private final ID identifier;
        private final List<AccessRequest<ID, VAL>> pendingAccess = new ArrayList<>();
        private DeleteOperation pendingDelete = null;
        private boolean pendingShutdown = false;

        LoadingStateTracker(ID identifier) {
            this.identifier = identifier;
        }

        public void start(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor) {
            op.start((id, val) -> {
                synchronized (Loader.this.lock) {
                    Loader.this.trackers.remove(this.identifier);

                    ActiveStateTracker activeTracker = new ActiveStateTracker(id, val);

                    activeTracker.provisionAccess(accessor);
                    for (var request : this.pendingAccess) {
                        activeTracker.provisionAccess(request.accessor());
                    }

                    if (this.pendingShutdown) {
                        if (activeTracker.accessors.isEmpty()) {
                            if (activeTracker.anyRequiresSave) {
                                UnloadingStateTracker unloadTracker = new UnloadingStateTracker(id, val);
                                Loader.this.trackers.put(id, unloadTracker);
                                SaveOperation opSave = Loader.this.save(id, val);
                                unloadTracker.start(opSave);
                            }
                        } else {
                            activeTracker.shutdown();
                            Loader.this.trackers.put(id, activeTracker);
                        }
                    } else if (this.pendingDelete != null) {
                        if (activeTracker.accessors.isEmpty()) {
                            Loader.this.deleteReplaceTracker(id, this.pendingDelete, new ArrayList<>());
                        } else {
                            activeTracker.delete(this.pendingDelete);
                            Loader.this.trackers.put(id, activeTracker);
                        }
                    } else {
                        if (activeTracker.accessors.isEmpty()) {
                            if (activeTracker.anyRequiresSave) {
                                UnloadingStateTracker unloadTracker = new UnloadingStateTracker(id, val);
                                Loader.this.trackers.put(id, unloadTracker);
                                SaveOperation opSave = Loader.this.save(id, val);
                                unloadTracker.start(opSave);
                            }
                        } else {
                            Loader.this.trackers.put(id, activeTracker);
                        }
                    }
                }
            }, () -> {
                List<AccessRequest<ID, VAL>> snapshot;
                synchronized (Loader.this.lock) {
                    Loader.this.trackers.remove(this.identifier);
                    snapshot = new ArrayList<>(this.pendingAccess);
                }
                accessor.onNotFound();
                for (var request : snapshot) {
                    request.accessor().onNotFound();
                }
            });
        }

        @Override
        public void access(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor) {
            this.pendingAccess.add(new AccessRequest<>(op, accessor));
        }

        @Override
        public void delete(DeleteOperation op) {
            this.pendingDelete = op;
        }

        @Override
        public void shutdown() {
            this.pendingShutdown = true;
        }

    }

    private class ActiveStateTracker implements StateTracker<ID, VAL> {

        private final ActiveStateTracker athis = this;

        private final ID identifier;
        private final VAL value;

        private Substate<ID, VAL> substate = new ActiveSubstate();
        private boolean anyRequiresSave = false;

        private final Set<Accessor<ID, VAL>> accessors = new HashSet<>();

        private static interface Substate<ID, VAL> {
            void access(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor);
            void delete(DeleteOperation op);
            void shutdown();
            void done(Accessor<ID, VAL> accessor);
        }

        private class ActiveSubstate implements Substate<ID, VAL> {

            @Override
            public void access(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor) {
                athis.provisionAccess(accessor);
            }

            @Override
            public void delete(DeleteOperation op) {
                athis.substate = new DeletingSubstate(op);
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
                        UnloadingStateTracker unloadTracker = new UnloadingStateTracker(athis.identifier, athis.value);
                        Loader.this.trackers.put(athis.identifier, unloadTracker);
                        SaveOperation opSave = Loader.this.save(athis.identifier, athis.value);
                        unloadTracker.start(opSave);
                    } else {
                        Loader.this.trackers.remove(athis.identifier);
                    }
                }
            }

        }

        private class DeletingSubstate implements Substate<ID, VAL> {

            private final DeleteOperation deleteOp;
            private final List<AccessRequest<ID, VAL>> pendingAccess = new ArrayList<>();

            DeletingSubstate(DeleteOperation op) {
                this.deleteOp = op;
            }

            @Override
            public void access(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor) {
                this.pendingAccess.add(new AccessRequest<>(op, accessor));
            }

            @Override
            public void delete(DeleteOperation op) {}

            @Override
            public void shutdown() {
                athis.substate = new ShutdownSubstate();
            }

            @Override
            public void done(Accessor<ID, VAL> accessor) {
                athis.accessors.remove(accessor);

                if (athis.accessors.isEmpty()) {
                    Loader.this.deleteReplaceTracker(athis.identifier, this.deleteOp, this.pendingAccess);
                }
            }

        }

        private class ShutdownSubstate implements Substate<ID, VAL> {

            @Override
            public void access(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor) {}

            @Override
            public void delete(DeleteOperation op) {}

            @Override
            public void shutdown() {}

            @Override
            public void done(Accessor<ID, VAL> accessor) {
                athis.accessors.remove(accessor);

                if (athis.accessors.isEmpty()) {
                    if (athis.anyRequiresSave) {
                        UnloadingStateTracker unloadTracker = new UnloadingStateTracker(athis.identifier, athis.value);
                        Loader.this.trackers.put(athis.identifier, unloadTracker);
                        SaveOperation opSave = Loader.this.save(athis.identifier, athis.value);
                        unloadTracker.start(opSave);
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
        public void access(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor) {
            this.substate.access(op, accessor);
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
        public void delete(DeleteOperation op) {
            this.substate.delete(op);
        }

        @Override
        public void shutdown() {
            this.substate.shutdown();
        }

    }

    private class UnloadingStateTracker implements StateTracker<ID, VAL> {

        private final ID identifier;
        private final VAL value;

        private final List<AccessRequest<ID, VAL>> pendingAccess = new ArrayList<>();

        private DeleteOperation pendingDelete = null;
        private boolean pendingShutdown = false;

        UnloadingStateTracker(ID identifier, VAL value) {
            this.identifier = identifier;
            this.value = value;
        }

        @Override
        public void access(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor) {
            this.pendingAccess.add(new AccessRequest<>(op, accessor));
        }

        // Under Lock
        public void onComplete() {
            Loader.this.trackers.remove(this.identifier);

            if (this.pendingShutdown) {
                this.pendingAccess.clear();
                this.pendingDelete = null;
            } else if (this.pendingDelete != null) {
                Loader.this.deleteReplaceTracker(this.identifier, this.pendingDelete, this.pendingAccess);
            } else if (!this.pendingAccess.isEmpty()) {
                ActiveStateTracker activeTracker
                    = new ActiveStateTracker(this.identifier, this.value);

                var roller = new RuntimeExceptionRoller();

                for (var request : this.pendingAccess) {
                    roller.exec(() -> activeTracker.provisionAccess(request.accessor()));
                }

                boolean doneDuringInit = activeTracker.accessors.isEmpty();

                if (doneDuringInit) {
                    if (activeTracker.anyRequiresSave) {
                        UnloadingStateTracker unloadTracker = new UnloadingStateTracker(this.identifier, this.value);
                        Loader.this.trackers.put(this.identifier, unloadTracker);
                        SaveOperation opSave = Loader.this.save(this.identifier, this.value);
                        unloadTracker.start(opSave);
                    }
                } else {
                    Loader.this.trackers.put(this.identifier, activeTracker);
                }

                roller.raise();
            }
        }

        @Override
        public void delete(DeleteOperation op) {
            this.pendingDelete = op;
        }

        @Override
        public void shutdown() {
            this.pendingShutdown = true;
        }

        public void start(SaveOperation opSave) {
            opSave.start(() -> {
                synchronized (Loader.this.lock) {
                    this.onComplete();
                }
            });
        }

    }

    private class DeletingStateTracker implements StateTracker<ID, VAL> {

        private final ID identifier;

        private final List<AccessRequest<ID, VAL>> pendingAccess = new ArrayList<>();

        private boolean pendingShutdown = false;

        public DeletingStateTracker(ID identifier) {
            this.identifier = identifier;
        }

        @Override
        public void access(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor) {
            this.pendingAccess.add(new AccessRequest<>(op, accessor));
        }

        @Override
        public void delete(DeleteOperation op) {}

        @Override
        public void shutdown() {
            this.pendingShutdown = true;
        }

        public void onComplete() {
            Loader.this.trackers.remove(this.identifier);

            if (this.pendingShutdown) {
                this.pendingAccess.clear();
                return;
            }

            for (var request : this.pendingAccess) {
                Loader.this.access(this.identifier, request.op(), request.accessor());
            }
        }

    }

    public static class CacheCollisionException extends IllegalStateException {}
    
    protected void access(
        AccessOperation<ID, VAL> op, 
        Accessor<ID, VAL> accessor
    ) {
        synchronized (this.lock) {
            this.checkShutdown();

            op.start((id, val) -> {
                synchronized (this.lock) {
                    StateTracker<ID, VAL> tracker = this.trackers.get(id);

                    if (tracker == null) {
                        ActiveStateTracker activeTracker = new ActiveStateTracker(id, val);

                        boolean doneDuringInit = activeTracker.provisionAccess(accessor);

                        if (doneDuringInit) {
                            if (activeTracker.anyRequiresSave) {
                                UnloadingStateTracker unloadTracker = new UnloadingStateTracker(id, val);
                                this.trackers.put(id, unloadTracker);

                                SaveOperation opSave = this.save(id, val);
                                unloadTracker.start(opSave);
                            }
                        } else {
                            Loader.this.trackers.put(id, activeTracker);
                        }
                    } else {
                        throw new CacheCollisionException();
                    }
                }
            }, accessor::onNotFound);
        }
    }

    protected void access(
        ID cachedIdentifier, 
        AccessOperation<ID, VAL> op, 
        Accessor<ID, VAL> accessor
    ) {
        synchronized (this.lock) {
            this.checkShutdown();
            
            StateTracker<ID, VAL> tracker = this.trackers.get(cachedIdentifier);

            if (tracker == null) {
                LoadingStateTracker loadingTracker = new LoadingStateTracker(cachedIdentifier);
                this.trackers.put(cachedIdentifier, loadingTracker);

                loadingTracker.start(op, accessor);
            } else {
                tracker.access(op, accessor);
            }
        }
    }

    protected void delete(
        ID identifier,
        DeleteOperation op
    ) {
        synchronized (this.lock) {
            this.checkShutdown();

            StateTracker<ID, VAL> tracker = this.trackers.get(identifier);

            if (tracker == null) {
                this.deleteReplaceTracker(identifier, op, new ArrayList<>());
            } else {
                tracker.delete(op);
            }
        }
    }

    protected abstract SaveOperation save(ID identifier, VAL value);

}
