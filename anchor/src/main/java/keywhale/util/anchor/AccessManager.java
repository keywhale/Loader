package keywhale.util.anchor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public final class AccessManager<ID, VAL> {

    private final BiFunction<ID, VAL, SaveOperation> save;

    private final Object lock = new Object();
    private final Map<ID, StateTracker<ID, VAL>> trackers = new HashMap<>();
    private boolean isShutdown = false;

    public AccessManager(BiFunction<ID, VAL, SaveOperation> save) {
        this.save = save;
    }

    public AccessManager() {
        this(null);
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
    private void deleteReplaceTracker(ID identifier, DeleteRequest deleteRequest, Collection<AccessRequest<ID, VAL>> accessors) {
        DeletingStateTracker deleteTracker = new DeletingStateTracker(identifier);
        this.trackers.put(identifier, deleteTracker);

        deleteTracker.pendingAccess.addAll(accessors);

        deleteRequest.op().start().whenComplete((result, ex) -> {
            synchronized (this.lock) {
                if (ex != null) {
                    Throwable cause = ex instanceof CompletionException ? ex.getCause() : ex;
                    if (deleteRequest.deleter() != null) {
                        if (cause instanceof DeleteOperation.NotFound) {
                            deleteRequest.deleter().fail(new AttemptFailedException.NotFound());
                        } else if (cause instanceof RuntimeException re) {
                            deleteRequest.deleter().fail(new AttemptFailedException.Other(re));
                        }
                    }
                } else {
                    if (deleteRequest.deleter() != null) deleteRequest.deleter().done();
                }
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

    private record DeleteRequest(
        DeleteOperation op,
        Deleter deleter
    ) {}

    private interface StateTracker<ID, VAL> {
        void access(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor);
        void delete(DeleteOperation op, Deleter deleter);
        void shutdown();
    }

    private class LoadingStateTracker implements StateTracker<ID, VAL> {

        private final ID identifier;
        private final List<AccessRequest<ID, VAL>> pendingAccess = new ArrayList<>();
        private DeleteRequest pendingDelete = null;
        private boolean pendingShutdown = false;

        LoadingStateTracker(ID identifier) {
            this.identifier = identifier;
        }

        public void start(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor) {
            op.start().whenComplete((result, ex) -> {
                if (ex != null) {
                    Throwable cause = ex instanceof CompletionException ? ex.getCause() : ex;
                    AttemptFailedException failure;
                    if (cause instanceof AccessOperation.NotFound) {
                        failure = new AttemptFailedException.NotFound();
                    } else if (cause instanceof RuntimeException re) {
                        failure = new AttemptFailedException.Other(re);
                    } else {
                        return;
                    }
                    List<AccessRequest<ID, VAL>> snapshot;
                    DeleteRequest deleteSnapshot;
                    synchronized (AccessManager.this.lock) {
                        AccessManager.this.trackers.remove(this.identifier);
                        snapshot = new ArrayList<>(this.pendingAccess);
                        deleteSnapshot = this.pendingDelete;
                    }
                    accessor.fail(failure);
                    for (var request : snapshot) {
                        request.accessor().fail(failure);
                    }
                    if (deleteSnapshot != null && deleteSnapshot.deleter() != null) {
                        deleteSnapshot.deleter().fail(failure);
                    }
                    return;
                }

                ID id = result.id();
                VAL val = result.value();
                synchronized (AccessManager.this.lock) {
                    ActiveStateTracker activeTracker = new ActiveStateTracker(id, val);
                    AccessManager.this.trackers.put(id, activeTracker);

                    activeTracker.provisionAccess(accessor);
                    for (var request : this.pendingAccess) {
                        activeTracker.provisionAccess(request.accessor());
                    }

                    boolean doneDuringInit = activeTracker.accessors.isEmpty();

                    if (this.pendingShutdown) {
                        if (this.pendingDelete != null && this.pendingDelete.deleter() != null) {
                            this.pendingDelete.deleter().fail(new AttemptFailedException.ShuttingDown());
                        }
                        if (doneDuringInit) {
                            if (activeTracker.anyRequiresSave) {
                                UnloadingStateTracker unloadTracker = new UnloadingStateTracker(id, val);
                                AccessManager.this.trackers.put(id, unloadTracker);
                                SaveOperation opSave = AccessManager.this.save(id, val);
                                unloadTracker.start(opSave);
                            } else {
                                AccessManager.this.trackers.remove(id);
                            }
                        } else {
                            activeTracker.shutdown();
                        }
                    } else if (this.pendingDelete != null) {
                        if (doneDuringInit) {
                            AccessManager.this.deleteReplaceTracker(id, this.pendingDelete, new ArrayList<>());
                        } else {
                            activeTracker.delete(this.pendingDelete.op(), this.pendingDelete.deleter());
                        }
                    } else {
                        if (doneDuringInit) {
                            if (activeTracker.anyRequiresSave) {
                                UnloadingStateTracker unloadTracker = new UnloadingStateTracker(id, val);
                                AccessManager.this.trackers.put(id, unloadTracker);
                                SaveOperation opSave = AccessManager.this.save(id, val);
                                unloadTracker.start(opSave);
                            } else {
                                AccessManager.this.trackers.remove(id);
                            }
                        }
                    }
                }
            });
        }

        @Override
        public void access(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor) {
            this.pendingAccess.add(new AccessRequest<>(op, accessor));
        }

        @Override
        public void delete(DeleteOperation op, Deleter deleter) {
            this.pendingDelete = new DeleteRequest(op, deleter);
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
            void delete(DeleteOperation op, Deleter deleter);
            void shutdown();
            void done(Accessor<ID, VAL> accessor);
        }

        private class ActiveSubstate implements Substate<ID, VAL> {

            @Override
            public void access(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor) {
                athis.provisionAccess(accessor);
            }

            @Override
            public void delete(DeleteOperation op, Deleter deleter) {
                athis.substate = new DeletingSubstate(op, deleter);
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
                        AccessManager.this.trackers.put(athis.identifier, unloadTracker);
                        SaveOperation opSave = AccessManager.this.save(athis.identifier, athis.value);
                        unloadTracker.start(opSave);
                    } else {
                        AccessManager.this.trackers.remove(athis.identifier);
                    }
                }
            }

        }

        private class DeletingSubstate implements Substate<ID, VAL> {

            private final DeleteRequest deleteRequest;
            private final List<AccessRequest<ID, VAL>> pendingAccess = new ArrayList<>();

            DeletingSubstate(DeleteOperation op, Deleter deleter) {
                this.deleteRequest = new DeleteRequest(op, deleter);
            }

            @Override
            public void access(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor) {
                this.pendingAccess.add(new AccessRequest<>(op, accessor));
            }

            @Override
            public void delete(DeleteOperation op, Deleter deleter) {
                if (deleter != null) deleter.fail(new AttemptFailedException.Deleting());
            }

            @Override
            public void shutdown() {
                if (this.deleteRequest.deleter() != null) {
                    this.deleteRequest.deleter().fail(new AttemptFailedException.ShuttingDown());
                }
                for (var request : this.pendingAccess) {
                    request.accessor().fail(new AttemptFailedException.ShuttingDown());
                }
                this.pendingAccess.clear();
                athis.substate = new ShutdownSubstate();
            }

            @Override
            public void done(Accessor<ID, VAL> accessor) {
                athis.accessors.remove(accessor);

                if (athis.accessors.isEmpty()) {
                    AccessManager.this.deleteReplaceTracker(athis.identifier, this.deleteRequest, this.pendingAccess);
                }
            }

        }

        private class ShutdownSubstate implements Substate<ID, VAL> {

            @Override
            public void access(AccessOperation<ID, VAL> op, Accessor<ID, VAL> accessor) {
                accessor.fail(new AttemptFailedException.ShuttingDown());
            }

            @Override
            public void delete(DeleteOperation op, Deleter deleter) {
                if (deleter != null) deleter.fail(new AttemptFailedException.ShuttingDown());
            }

            @Override
            public void shutdown() {}

            @Override
            public void done(Accessor<ID, VAL> accessor) {
                athis.accessors.remove(accessor);

                if (athis.accessors.isEmpty()) {
                    if (athis.anyRequiresSave) {
                        UnloadingStateTracker unloadTracker = new UnloadingStateTracker(athis.identifier, athis.value);
                        AccessManager.this.trackers.put(athis.identifier, unloadTracker);
                        SaveOperation opSave = AccessManager.this.save(athis.identifier, athis.value);
                        unloadTracker.start(opSave);
                    } else {
                        AccessManager.this.trackers.remove(athis.identifier);
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

        private void provisionAccess(Accessor<ID, VAL> accessor) {
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

                        if (isInit.get()) {
                            doneDuringInit.set(true);
                        } else if (!initSuccess.get()) {
                            return;
                        } else {
                            synchronized (AccessManager.this.lock) {
                                athis.substate.done(accessor);
                            }
                        }
                    }
                }

                @Override
                public void save() {
                    synchronized (AccessManager.this.lock) {
                        athis.anyRequiresSave = true;
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
        }

        @Override
        public void delete(DeleteOperation op, Deleter deleter) {
            this.substate.delete(op, deleter);
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

        private DeleteRequest pendingDelete = null;
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
            AccessManager.this.trackers.remove(this.identifier);

            if (this.pendingShutdown) {
                for (var request : this.pendingAccess) {
                    request.accessor().fail(new AttemptFailedException.ShuttingDown());
                }
                this.pendingAccess.clear();
                if (this.pendingDelete != null && this.pendingDelete.deleter() != null) {
                    this.pendingDelete.deleter().fail(new AttemptFailedException.ShuttingDown());
                }
                this.pendingDelete = null;
            } else if (this.pendingDelete != null) {
                AccessManager.this.deleteReplaceTracker(this.identifier, this.pendingDelete, this.pendingAccess);
            } else if (!this.pendingAccess.isEmpty()) {
                ActiveStateTracker activeTracker
                    = new ActiveStateTracker(this.identifier, this.value);
                AccessManager.this.trackers.put(this.identifier, activeTracker);

                var roller = new RuntimeExceptionRoller();

                for (var request : this.pendingAccess) {
                    roller.exec(() -> activeTracker.provisionAccess(request.accessor()));
                }

                boolean doneDuringInit = activeTracker.accessors.isEmpty();

                if (doneDuringInit) {
                    if (activeTracker.anyRequiresSave) {
                        UnloadingStateTracker unloadTracker = new UnloadingStateTracker(this.identifier, this.value);
                        AccessManager.this.trackers.put(this.identifier, unloadTracker);
                        SaveOperation opSave = AccessManager.this.save(this.identifier, this.value);
                        unloadTracker.start(opSave);
                    } else {
                        AccessManager.this.trackers.remove(this.identifier);
                    }
                }

                roller.raise();
            }
        }

        @Override
        public void delete(DeleteOperation op, Deleter deleter) {
            this.pendingDelete = new DeleteRequest(op, deleter);
        }

        @Override
        public void shutdown() {
            this.pendingShutdown = true;
        }

        public void start(SaveOperation opSave) {
            opSave.start().whenComplete((result, ex) -> {
                synchronized (AccessManager.this.lock) {
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
        public void delete(DeleteOperation op, Deleter deleter) {
            if (deleter != null) deleter.fail(new AttemptFailedException.Deleting());
        }

        @Override
        public void shutdown() {
            this.pendingShutdown = true;
        }

        public void onComplete() {
            AccessManager.this.trackers.remove(this.identifier);

            if (this.pendingShutdown) {
                for (var request : this.pendingAccess) {
                    request.accessor().fail(new AttemptFailedException.ShuttingDown());
                }
                this.pendingAccess.clear();
                return;
            }

            for (var request : this.pendingAccess) {
                AccessManager.this.access(this.identifier, request.op(), request.accessor());
            }
        }

    }

    public static class CacheCollisionException extends IllegalStateException {}
    
    public void access(
        AccessOperation<ID, VAL> op, 
        Accessor<ID, VAL> accessor
    ) {
        synchronized (this.lock) {
            if (this.isShutdown) {
                accessor.fail(new AttemptFailedException.ShuttingDown());
                return;
            }

            op.start().whenComplete((result, ex) -> {
                if (ex != null) {
                    Throwable cause = ex instanceof CompletionException ? ex.getCause() : ex;
                    if (cause instanceof AccessOperation.NotFound) {
                        accessor.fail(new AttemptFailedException.NotFound());
                    } else if (cause instanceof RuntimeException re) {
                        accessor.fail(new AttemptFailedException.Other(re));
                    }
                    return;
                }
                synchronized (this.lock) {
                    ID id = result.id();
                    VAL val = result.value();
                    StateTracker<ID, VAL> tracker = this.trackers.get(id);

                    if (tracker == null) {
                        ActiveStateTracker activeTracker = new ActiveStateTracker(id, val);
                        AccessManager.this.trackers.put(id, activeTracker);

                        activeTracker.provisionAccess(accessor);

                        boolean doneDuringInit = activeTracker.accessors.isEmpty();

                        if (doneDuringInit) {
                            if (activeTracker.anyRequiresSave) {
                                UnloadingStateTracker unloadTracker = new UnloadingStateTracker(id, val);
                                this.trackers.put(id, unloadTracker);

                                SaveOperation opSave = this.save(id, val);
                                unloadTracker.start(opSave);
                            } else {
                                this.trackers.remove(id);
                            }
                        }
                    } else {
                        accessor.fail(new AttemptFailedException.Other(new CacheCollisionException()));
                    }
                }
            });
        }
    }

    public void access(
        ID cachedIdentifier, 
        AccessOperation<ID, VAL> op, 
        Accessor<ID, VAL> accessor
    ) {
        synchronized (this.lock) {
            if (this.isShutdown) {
                accessor.fail(new AttemptFailedException.ShuttingDown());
                return;
            }
            
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

    public void delete(
        ID identifier,
        DeleteOperation op
    ) {
        this.delete(identifier, op, null);
    }

    public void delete(
        ID identifier,
        DeleteOperation op,
        Deleter deleter
    ) {
        synchronized (this.lock) {
            if (this.isShutdown) {
                if (deleter != null) deleter.fail(new AttemptFailedException.ShuttingDown());
                return;
            }

            StateTracker<ID, VAL> tracker = this.trackers.get(identifier);

            if (tracker == null) {
                this.deleteReplaceTracker(identifier, new DeleteRequest(op, deleter), new ArrayList<>());
            } else {
                tracker.delete(op, deleter);
            }
        }
    }

    private SaveOperation save(ID identifier, VAL value) {
        SaveOperation op = () -> CompletableFuture.completedFuture(null);

        if (this.save != null) {
            SaveOperation op2 = this.save.apply(identifier, value);
            if (op2 != null) {
                op = op2;
            }
        }

        return op;
    }

    public void saveActive() {
        synchronized (this.lock) {
            var roller = new RuntimeExceptionRoller();

            for (StateTracker<ID, VAL> tracker : this.trackers.values()) {
                if (
                    tracker instanceof ActiveStateTracker activeTracker
                    && activeTracker.substate instanceof ActiveStateTracker.ActiveSubstate
                    && activeTracker.anyRequiresSave
                ) {
                    roller.exec(() -> {
                        activeTracker.provisionAccess(Accessor.of((access) -> {
                            this.save(access.id(), access.value()).start().whenComplete((r, e) -> access.done());
                            return null;
                        }, (exc) -> {}));
                    });
                }
            }

            roller.raise();
        }
    }

    public interface AccessOperation<ID, VAL> {
        public record Result<ID, VAL>(ID id, VAL value) {}
        public static class NotFound extends Exception {}

        public CompletableFuture<Result<ID, VAL>> start();
    }

    public interface DeleteOperation {
        public static class NotFound extends Exception {}

        public CompletableFuture<Void> start();
    }

    public interface SaveOperation {
        public CompletableFuture<Void> start();
    }

    public interface Access<ID, VAL> {
        public ID id();
        public VAL value();
        public void done();
        public void save();
    }

    public abstract static class AttemptFailedException extends Exception {
        public static class NotFound extends AttemptFailedException {}
        public static class ShuttingDown extends AttemptFailedException {}
        public static class Deleting extends AttemptFailedException {}
        public static class Other extends AttemptFailedException {
            private final RuntimeException cause;

            public Other(RuntimeException cause) {
                this.cause = cause;
                this.initCause(cause);
            }

            public RuntimeException getCause() {
                return this.cause;
            }
        }
    }

    public interface Accessor<ID, VAL> {
        public void init(Access<ID, VAL> access);
        public void cancel();
        public void fail(AttemptFailedException exc);

        public static <ID, VAL> Accessor<ID, VAL> of(
            Function<Access<ID, VAL>, Runnable> onAccess,
            Consumer<AttemptFailedException> onFailure
        ) {
            return new Accessor<ID, VAL>() {

                private Runnable cancel;

                @Override
                public void init(Access<ID, VAL> access) {
                    this.cancel = onAccess.apply(access);
                }

                @Override
                public void cancel() {
                    if (this.cancel != null) {
                        this.cancel.run();
                    }
                }

                @Override
                public void fail(AttemptFailedException exc) {
                    onFailure.accept(exc);
                }
                
            };
        }
    }

    public interface Deleter {
        public void done();
        public void fail(AttemptFailedException exc);

        public static Deleter of(
            Runnable onDone, 
            Consumer<AttemptFailedException> onFailure
        ) {
            return new Deleter() {

                @Override
                public void done() {
                    if (onDone != null) {
                        onDone.run();
                    }
                }

                @Override
                public void fail(AttemptFailedException exc) {
                    onFailure.accept(exc);
                }
                
            };
        }
    }

}
