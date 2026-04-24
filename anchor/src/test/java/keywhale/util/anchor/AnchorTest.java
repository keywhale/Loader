package keywhale.util.anchor;

import org.junit.jupiter.api.*;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

class AnchorTest {

    // ---- Infrastructure ----

    static class ControlledCore implements Anchor.Core<String, String> {
        final AtomicInteger loadCalls = new AtomicInteger();
        final Semaphore loadStarted = new Semaphore(0);
        final Semaphore loadGate = new Semaphore(0);
        volatile String loadResult = "value";
        volatile RuntimeException loadException = null;

        final AtomicInteger saveCalls = new AtomicInteger();
        final Semaphore saveStarted = new Semaphore(0);
        final Semaphore saveGate = new Semaphore(0);

        final AtomicInteger deleteCalls = new AtomicInteger();
        final Semaphore deleteStarted = new Semaphore(0);
        final Semaphore deleteGate = new Semaphore(0);
        volatile RuntimeException deleteException = null;

        @Override
        public String load(String id) {
            loadCalls.incrementAndGet();
            loadStarted.release();
            loadGate.acquireUninterruptibly();
            RuntimeException ex = loadException;
            if (ex != null) { loadException = null; throw ex; }
            return loadResult;
        }

        @Override
        public void save(String id, String val) {
            saveCalls.incrementAndGet();
            saveStarted.release();
            saveGate.acquireUninterruptibly();
        }

        @Override
        public void delete(String id) {
            deleteCalls.incrementAndGet();
            deleteStarted.release();
            deleteGate.acquireUninterruptibly();
            RuntimeException ex = deleteException;
            if (ex != null) { deleteException = null; throw ex; }
        }

        void allowLoad() { loadGate.release(); }
        void allowSave() { saveGate.release(); }
        void allowDelete() { deleteGate.release(); }

        void awaitLoadStarted() { acquire(loadStarted); }
        void awaitSaveStarted() { acquire(saveStarted); }
        void awaitDeleteStarted() { acquire(deleteStarted); }

        private static void acquire(Semaphore s) {
            try { assertTrue(s.tryAcquire(2, TimeUnit.SECONDS), "operation never started"); }
            catch (InterruptedException e) { fail("interrupted"); }
        }
    }

    static final ExecutorService pool = Executors.newCachedThreadPool();

    static <T> Future<T> async(Callable<T> c) { return pool.submit(c); }

    static Future<Void> asyncVoid(ThrowingRunnable r) {
        return pool.submit(() -> { r.run(); return null; });
    }

    @FunctionalInterface
    interface ThrowingRunnable { void run() throws Exception; }

    static void awaitBlocked(Thread t) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 2_000;
        while (System.currentTimeMillis() < deadline) {
            Thread.State s = t.getState();
            if (s == Thread.State.WAITING || s == Thread.State.TIMED_WAITING) return;
            Thread.sleep(1);
        }
        fail("Thread never blocked; state: " + t.getState());
    }

    static <T> T get(Future<T> f) {
        try { return f.get(2, TimeUnit.SECONDS); }
        catch (ExecutionException e) { sneakyThrow(e.getCause()); return null; }
        catch (Exception e) { return fail("future timed out or interrupted: " + e); }
    }

    static <T> void assertPending(Future<T> f) throws Exception {
        Thread.sleep(50);
        assertFalse(f.isDone(), "expected future to still be pending");
    }

    static <T> Class<? extends Throwable> asyncThrows(Future<T> f) {
        try { f.get(2, TimeUnit.SECONDS); return null; }
        catch (ExecutionException e) { return e.getCause().getClass(); }
        catch (Exception e) { return fail("unexpected: " + e); }
    }

    @SuppressWarnings("unchecked")
    static <E extends Throwable> void sneakyThrow(Throwable t) throws E { throw (E) t; }

    // Helper: get a thread reference out of an async task
    static class Async<T> {
        final Future<T> future;
        final Thread thread;
        Async(Future<T> f, Thread t) { future = f; thread = t; }
    }

    static <T> Async<T> asyncTracked(Callable<T> c) throws InterruptedException {
        AtomicReference<Thread> ref = new AtomicReference<>();
        CountDownLatch started = new CountDownLatch(1);
        Future<T> f = pool.submit(() -> {
            ref.set(Thread.currentThread());
            started.countDown();
            return c.call();
        });
        assertTrue(started.await(2, TimeUnit.SECONDS));
        return new Async<>(f, ref.get());
    }

    static Async<Void> asyncTrackedVoid(ThrowingRunnable r) throws InterruptedException {
        return asyncTracked(() -> { r.run(); return null; });
    }

    Anchor<String, String> lc;
    ControlledCore core;

    @BeforeEach
    void setUp() {
        core = new ControlledCore();
        lc = new Anchor<>(core);
    }

    // ---- Group A: Single-threaded happy path ----

    @Nested
    class A_Basic {

        @Test
        void A1_basicLoadAndAccess() {
            Future<String> f = async(() -> lc.access("x").value());
            core.allowLoad();
            assertEquals("value", get(f));
            assertEquals(1, core.loadCalls.get());
        }

        @Test
        void A2_closeWithoutSave_noSave() throws Exception {
            Future<Void> f = asyncVoid(() -> { lc.access("x").close(); });
            core.allowLoad();
            get(f);
            assertEquals(0, core.saveCalls.get());
        }

        @Test
        void A3_closeAfterSave_saveTriggered() throws Exception {
            Future<Void> f = asyncVoid(() -> {
                Anchor.Access<String, String> a = lc.access("x");
                a.save();
                a.close();
            });
            core.allowLoad();
            core.awaitSaveStarted();
            core.allowSave();
            get(f);
            assertEquals(1, core.saveCalls.get());
        }

        @Test
        void A4_multipleSaveCalls_onlyOneSave() throws Exception {
            Future<Void> f = asyncVoid(() -> {
                Anchor.Access<String, String> a = lc.access("x");
                a.save(); a.save(); a.save();
                a.close();
            });
            core.allowLoad();
            core.awaitSaveStarted();
            core.allowSave();
            get(f);
            assertEquals(1, core.saveCalls.get());
        }

        @Test
        void A5_deleteFromNull() throws Exception {
            Future<Void> f = asyncVoid(() -> lc.delete("x"));
            core.allowDelete();
            get(f);
            assertEquals(1, core.deleteCalls.get());
        }

        @Test
        void A6_deleteNotFoundException_propagates() {
            core.deleteException = new Anchor.Core.NotFoundException();
            Future<Void> f = asyncVoid(() -> lc.delete("x"));
            core.allowDelete();
            assertEquals(Anchor.Core.NotFoundException.class, asyncThrows(f));
        }

        @Test
        void A7_twoSequentialAccesses_twoLoads() throws Exception {
            Future<Void> f = asyncVoid(() -> {
                lc.access("x").close();
                lc.access("x").close();
            });
            core.allowLoad();
            core.allowLoad();
            get(f);
            assertEquals(2, core.loadCalls.get());
        }

        @Test
        void A8_accessAfterSave_reloads() throws Exception {
            Future<Void> f = asyncVoid(() -> {
                Anchor.Access<String, String> a = lc.access("x");
                a.save(); a.close();
                lc.access("x").close();
            });
            core.allowLoad();
            core.awaitSaveStarted(); core.allowSave();
            core.allowLoad();
            get(f);
            assertEquals(2, core.loadCalls.get());
            assertEquals(1, core.saveCalls.get());
        }
    }

    // ---- Group B: Concurrent access coalescing ----

    @Nested
    class B_Coalescing {

        @Test
        void B1_twoConcurrentAccesses_oneLoad() throws Exception {
            Async<String> a = asyncTracked(() -> lc.access("x").value());
            core.awaitLoadStarted();

            Future<String> b = async(() -> lc.access("x").value());
            awaitBlocked(a.thread); // A is inside load; B should park

            core.allowLoad();
            assertEquals("value", get(a.future));
            assertEquals("value", get(b));
            assertEquals(1, core.loadCalls.get());
        }

        @Test
        void B2_nConcurrentAccesses_oneLoad() throws Exception {
            int n = 10;
            List<Future<String>> futures = new ArrayList<>();

            Async<String> first = asyncTracked(() -> lc.access("x").value());
            core.awaitLoadStarted();

            for (int i = 1; i < n; i++) {
                futures.add(async(() -> lc.access("x").value()));
            }
            Thread.sleep(100); // let all park

            core.allowLoad();
            assertEquals("value", get(first.future));
            for (Future<String> f : futures) assertEquals("value", get(f));
            assertEquals(1, core.loadCalls.get());
        }

        @Test
        void B3_allPendingAccessesGetIndependentAccess() throws Exception {
            int n = 3;
            List<Future<Anchor.Access<String, String>>> futures = new ArrayList<>();

            Async<Anchor.Access<String, String>> first =
                asyncTracked(() -> lc.access("x"));
            core.awaitLoadStarted();

            for (int i = 1; i < n; i++) {
                futures.add(async(() -> lc.access("x")));
            }
            Thread.sleep(100);

            core.allowLoad();

            Anchor.Access<String, String> a0 = get(first.future);
            List<Anchor.Access<String, String>> accesses = new ArrayList<>();
            accesses.add(a0);
            for (Future<Anchor.Access<String, String>> f : futures) accesses.add(get(f));

            // Close all but last — no save yet
            for (int i = 0; i < n - 1; i++) accesses.get(i).close();
            assertEquals(0, core.saveCalls.get());

            // Mark dirty and close last — run on async thread since close triggers save
            Anchor.Access<String, String> last = accesses.get(n - 1);
            Future<Void> fClose = asyncVoid(() -> { last.save(); last.close(); });
            core.awaitSaveStarted(); core.allowSave();
            get(fClose);
            assertEquals(1, core.saveCalls.get());
        }
    }

    // ---- Group C: Save interactions ----

    @Nested
    class C_Save {

        @Test
        void C1_accessDuringSave_getsSameValueWithoutReload() throws Exception {
            // Thread A: access, save(), close → SavingState
            Future<Anchor.Access<String, String>> fA = async(() -> {
                Anchor.Access<String, String> a = lc.access("x");
                a.save(); a.close();
                return null; // A is done
            });
            core.allowLoad();
            core.awaitSaveStarted(); // save is running, state = SavingState

            // Thread B: access while saving → parks
            Future<String> fB = async(() -> lc.access("x").value());
            Thread.sleep(50);
            assertFalse(fB.isDone());

            core.allowSave(); // save completes → B wakes up with same value
            assertEquals("value", get(fB));

            // No reload happened
            assertEquals(1, core.loadCalls.get());
            assertEquals(1, core.saveCalls.get());
        }

        @Test
        void C2_deleteDuringSave_parksUntilSaveDone() throws Exception {
            Future<Void> fA = asyncVoid(() -> {
                Anchor.Access<String, String> a = lc.access("x");
                a.save(); a.close();
            });
            core.allowLoad();
            core.awaitSaveStarted();

            Future<Void> fB = asyncVoid(() -> lc.delete("x"));
            Thread.sleep(50);
            assertFalse(fB.isDone()); // B blocked waiting for save

            core.allowSave(); // save done → delete starts
            core.awaitDeleteStarted();
            core.allowDelete();

            get(fA);
            get(fB);
            assertEquals(1, core.saveCalls.get());
            assertEquals(1, core.deleteCalls.get());
        }

        @Test
        void C3_saveSkippedWhenDeletePending() throws Exception {
            // A holds access, marks dirty
            Future<Anchor.Access<String, String>> fA = async(() -> lc.access("x"));
            core.allowLoad();
            Anchor.Access<String, String> a = get(fA);
            a.save(); // marks dirty

            // B: delete → cancelling, parks
            Future<Void> fB = asyncVoid(() -> lc.delete("x"));
            Thread.sleep(50); // B should be parked (waiting for A to close)

            a.close(); // A closes → delete runs (save skipped because delete pending)
            core.awaitDeleteStarted();
            core.allowDelete();
            get(fB);

            assertEquals(0, core.saveCalls.get()); // save was NOT triggered
            assertEquals(1, core.deleteCalls.get());
        }
    }

    // ---- Group D: Delete interactions ----

    @Nested
    class D_Delete {

        @Test
        void D1_deleteWhileActive_onCancelSignaled() throws Exception {
            Future<Anchor.Access<String, String>> fA = async(() -> lc.access("x"));
            core.allowLoad();
            Anchor.Access<String, String> a = get(fA);
            Anchor.Event onCancel = a.getCancelEvent();

            assertFalse(onCancel.isSet());

            Future<Void> fB = asyncVoid(() -> lc.delete("x"));
            Thread.sleep(50); // B parked waiting for A to close

            assertTrue(onCancel.isSet()); // cancel was signaled

            a.close();
            core.awaitDeleteStarted(); core.allowDelete();
            get(fB);
            assertEquals(1, core.deleteCalls.get());
        }

        @Test
        void D2_deleteWhileActive_multipleAccessors_allCancelSignaled() throws Exception {
            Future<Anchor.Access<String, String>> fA = async(() -> lc.access("x"));
            core.awaitLoadStarted();
            Future<Anchor.Access<String, String>> fB = async(() -> lc.access("x"));
            core.allowLoad();

            Anchor.Access<String, String> a = get(fA);
            Anchor.Event onCancelA = a.getCancelEvent();
            Anchor.Access<String, String> b = get(fB);
            Anchor.Event onCancelB = b.getCancelEvent();

            Future<Void> fDel = asyncVoid(() -> lc.delete("x"));
            Thread.sleep(50);
            assertTrue(onCancelA.isSet());
            assertTrue(onCancelB.isSet());

            a.close(); // not last; delete still waiting
            Thread.sleep(50);
            assertFalse(fDel.isDone());

            b.close(); // last → delete runs
            core.awaitDeleteStarted(); core.allowDelete();
            get(fDel);
            assertEquals(1, core.deleteCalls.get());
        }

        @Test
        void D3_deleteCompletes_pendingAccess_becomesLoader() throws Exception {
            Async<Void> delAsync = asyncTrackedVoid(() -> lc.delete("x"));
            core.awaitDeleteStarted();

            Future<String> fB = async(() -> lc.access("x").value());
            awaitBlocked(delAsync.thread); // D is inside delete; B parks

            core.allowDelete(); // delete done → B becomes loader
            core.awaitLoadStarted();
            core.allowLoad();

            assertEquals("value", get(fB));
            assertEquals(1, core.deleteCalls.get());
            assertEquals(1, core.loadCalls.get());
        }

        @Test
        void D4_deleteCompletes_nothingPending_nextAccessReloads() throws Exception {
            Future<Void> f = asyncVoid(() -> lc.delete("x"));
            core.allowDelete();
            get(f);
            assertEquals(1, core.deleteCalls.get());
            assertEquals(0, core.loadCalls.get());

            Future<String> f2 = async(() -> lc.access("x").value());
            core.allowLoad();
            assertEquals("value", get(f2));
            assertEquals(1, core.loadCalls.get());
        }

        @Test
        void D5_twoConcurrentDeletes_bothRun() throws Exception {
            Async<Void> delA = asyncTrackedVoid(() -> lc.delete("x"));
            core.awaitDeleteStarted();

            Future<Void> delB = asyncVoid(() -> lc.delete("x"));
            awaitBlocked(delA.thread);

            core.allowDelete(); // A done → B runs
            core.awaitDeleteStarted();
            core.allowDelete(); // B done

            get(delA.future);
            get(delB);
            assertEquals(2, core.deleteCalls.get());
        }

        @Test
        void D6_deleteWhileLoading() throws Exception {
            // A: access → loading (paused)
            Async<Anchor.Access<String, String>> accAsync =
                asyncTracked(() -> lc.access("x"));
            core.awaitLoadStarted();

            // B: delete → parks in pendingDelete
            Async<Void> delAsync = asyncTrackedVoid(() -> lc.delete("x"));
            awaitBlocked(delAsync.thread);

            // Load succeeds → A re-parks, delete runs
            core.allowLoad();
            core.awaitDeleteStarted();
            core.allowDelete(); // delete done → A resumes as loader

            // A runs second load
            core.awaitLoadStarted();
            core.allowLoad();

            Anchor.Access<String, String> a = get(accAsync.future);
            get(delAsync.future);

            assertNotNull(a);
            assertEquals("value", a.value());
            a.close();

            assertEquals(2, core.loadCalls.get());
            assertEquals(1, core.deleteCalls.get());
        }

        @Test
        void D7_accessDuringDeletion_parksAndLoadsAfter() throws Exception {
            Async<Void> delAsync = asyncTrackedVoid(() -> lc.delete("x"));
            core.awaitDeleteStarted();

            Future<String> fB = async(() -> lc.access("x").value());
            awaitBlocked(delAsync.thread);

            core.allowDelete(); // delete done → B becomes loader
            core.awaitLoadStarted();
            core.allowLoad();

            assertEquals("value", get(fB));
            assertEquals(1, core.deleteCalls.get());
            assertEquals(1, core.loadCalls.get());
        }
    }

    // ---- Group E: Load failure ----

    @Nested
    class E_LoadFailure {

        @Test
        void E1_loadFails_nothingPending_exceptionPropagates() {
            core.loadException = new RuntimeException("load failed");
            Future<Void> f = asyncVoid(() -> lc.access("x").close());
            core.allowLoad();
            assertEquals(RuntimeException.class, asyncThrows(f));
            assertEquals(1, core.loadCalls.get());
        }

        @Test
        void E1b_afterLoadFailure_nextAccessReloads() throws Exception {
            core.loadException = new RuntimeException("load failed");
            Future<Void> f1 = asyncVoid(() -> lc.access("x").close());
            core.allowLoad();
            asyncThrows(f1); // consume exception

            Future<String> f2 = async(() -> lc.access("x").value());
            core.allowLoad();
            assertEquals("value", get(f2));
            assertEquals(2, core.loadCalls.get());
        }

        @Test
        void E2_loadFails_pendingAccess_pendingBecomesLoader() throws Exception {
            // A: access → loading, fails
            Async<String> a = asyncTracked(() -> lc.access("x").value());
            core.awaitLoadStarted();

            // B: access → parks
            Future<String> b = async(() -> lc.access("x").value());
            awaitBlocked(a.thread);

            core.loadException = new RuntimeException("load failed");
            core.allowLoad(); // A fails, B becomes loader

            // A should throw
            assertEquals(RuntimeException.class, asyncThrows(a.future));

            // B now loads
            core.awaitLoadStarted();
            core.allowLoad();
            assertEquals("value", get(b));
            assertEquals(2, core.loadCalls.get());
        }

        @Test
        void E3_loadFails_pendingDelete_deleteRunsFirst() throws Exception {
            Async<String> a = asyncTracked(() -> lc.access("x").value());
            core.awaitLoadStarted();

            // B: access → pendingAccess
            Future<String> b = async(() -> lc.access("x").value());
            Thread.sleep(30);

            // C: delete → pendingDelete
            Future<Void> c = asyncVoid(() -> lc.delete("x"));
            Thread.sleep(30);

            core.loadException = new RuntimeException("load failed");
            core.allowLoad(); // A fails, C (delete) runs first

            assertEquals(RuntimeException.class, asyncThrows(a.future));

            core.awaitDeleteStarted();
            core.allowDelete(); // delete done → B becomes loader

            core.awaitLoadStarted();
            core.allowLoad();

            assertEquals("value", get(b));
            get(c);
            assertEquals(2, core.loadCalls.get());
            assertEquals(1, core.deleteCalls.get());
        }

        @Test
        void E4_cascadingLoadFailures() throws Exception {
            Async<String> a = asyncTracked(() -> lc.access("x").value());
            core.awaitLoadStarted();

            Future<String> b = async(() -> lc.access("x").value());
            awaitBlocked(a.thread);

            // Both loads fail
            core.loadException = new RuntimeException("fail 1");
            core.allowLoad(); // A fails, B becomes loader

            assertEquals(RuntimeException.class, asyncThrows(a.future));

            core.awaitLoadStarted();
            core.loadException = new RuntimeException("fail 2");
            core.allowLoad(); // B fails

            assertEquals(RuntimeException.class, asyncThrows(b));
            assertEquals(2, core.loadCalls.get());
        }
    }

    // ---- Group F: Load success with pending delete (re-park scenario) ----

    @Nested
    class F_LoadSuccessWithPendingDelete {

        @Test
        void F1_loaderReParks_deleteRuns_thenLoaderLoadsAgain() throws Exception {
            // A: access → loading, paused
            Async<Anchor.Access<String, String>> a =
                asyncTracked(() -> lc.access("x"));
            core.awaitLoadStarted();

            // B: delete → parks in pendingDelete
            Future<Void> b = asyncVoid(() -> lc.delete("x"));
            awaitBlocked(a.thread);

            // Load succeeds: A re-parks, delete runs
            core.allowLoad();
            core.awaitDeleteStarted();

            // A is now parked again (inside pendingAccess), delete is running
            assertFalse(a.future.isDone());

            core.allowDelete(); // delete done → A resumes as loader

            core.awaitLoadStarted(); // A is loading again
            core.allowLoad();

            Anchor.Access<String, String> access = get(a.future);
            get(b);

            assertNotNull(access);
            assertEquals("value", access.value());
            access.close();

            assertEquals(2, core.loadCalls.get());
            assertEquals(1, core.deleteCalls.get());
        }

        @Test
        void F2_loaderAndPendingBothGetAccess_afterDelete() throws Exception {
            // A: access → loading, paused
            Async<Anchor.Access<String, String>> a =
                asyncTracked(() -> lc.access("x"));
            core.awaitLoadStarted();

            // B: access → pendingAccess
            Future<Anchor.Access<String, String>> b = async(() -> lc.access("x"));
            Thread.sleep(30);

            // C: delete → pendingDelete
            Async<Void> c = asyncTrackedVoid(() -> lc.delete("x"));
            awaitBlocked(c.thread); // confirm delete is parked before releasing load

            // Load succeeds: A re-parks (joins B in pendingAccess), C runs delete
            core.allowLoad();
            core.awaitDeleteStarted();

            assertFalse(a.future.isDone());
            assertFalse(b.isDone());

            core.allowDelete(); // delete done → one of {A,B} becomes loader
            core.awaitLoadStarted();
            core.allowLoad(); // second load succeeds → both A and B get Access

            Anchor.Access<String, String> aAccess = get(a.future);
            Anchor.Access<String, String> bAccess = get(b);
            get(c.future);

            assertNotNull(aAccess); assertEquals("value", aAccess.value());
            assertNotNull(bAccess); assertEquals("value", bAccess.value());

            aAccess.close();
            bAccess.close();

            assertEquals(2, core.loadCalls.get());
            assertEquals(1, core.deleteCalls.get());
        }
    }

    // ---- Group G: Shutdown ----

    @Nested
    class G_Shutdown {

        @Test
        void G1_shutdownThenAccess_throws() {
            lc.shutdown();
            assertThrows(Anchor.ShuttingDownException.class,
                () -> lc.access("x"));
        }

        @Test
        void G2_shutdownThenDelete_throws() {
            lc.shutdown();
            assertThrows(Anchor.ShuttingDownException.class,
                () -> lc.delete("x"));
        }

        @Test
        void G3_shutdownWhileActive_onCancelSignaled() throws Exception {
            Future<Anchor.Access<String, String>> f = async(() -> lc.access("x"));
            core.allowLoad();
            Anchor.Access<String, String> a = get(f);
            Anchor.Event onCancel = a.getCancelEvent();

            assertFalse(onCancel.isSet());
            lc.shutdown();
            assertTrue(onCancel.isSet());

            a.close(); // should not throw
        }

        @Test
        void G4_shutdownWhileLoading_parkedAccessGetsShuttingDown() throws Exception {
            Async<Anchor.Access<String, String>> a =
                asyncTracked(() -> lc.access("x"));
            core.awaitLoadStarted();

            Future<String> b = async(() -> lc.access("x").value());
            awaitBlocked(a.thread);

            lc.shutdown(); // signals B with SHUTDOWN

            assertEquals(Anchor.ShuttingDownException.class, asyncThrows(b));

            // A (in-flight load) completes normally
            core.allowLoad();
            Anchor.Access<String, String> aAccess = get(a.future);
            assertNotNull(aAccess);
            aAccess.close(); // no exception
        }

        @Test
        void G5_shutdownWhileActive_accessorCanStillClose() throws Exception {
            Future<Anchor.Access<String, String>> f = async(() -> lc.access("x"));
            core.allowLoad();
            Anchor.Access<String, String> a = get(f);
            Anchor.Event onCancel = a.getCancelEvent();

            lc.shutdown();
            assertTrue(onCancel.isSet());

            // close should not throw, save should not be triggered
            a.save();
            a.close();
            assertEquals(0, core.saveCalls.get()); // shutdown: state cleared, no save
        }

        @Test
        void G6_shutdownWhileSaving_parkedDeleteGetsShuttingDown() throws Exception {
            Future<Void> fA = asyncVoid(() -> {
                Anchor.Access<String, String> a = lc.access("x");
                a.save(); a.close();
            });
            core.allowLoad();
            core.awaitSaveStarted();

            Future<Void> fB = asyncVoid(() -> lc.delete("x"));
            Thread.sleep(50); // B parks in pendingDelete

            lc.shutdown(); // signals B with SHUTDOWN
            assertEquals(Anchor.ShuttingDownException.class, asyncThrows(fB));

            core.allowSave(); // A's save completes (in-flight, not interrupted)
            get(fA);
        }

        @Test
        void G7_inFlightLoadNotInterruptedByShutdown() throws Exception {
            Async<Anchor.Access<String, String>> a =
                asyncTracked(() -> lc.access("x"));
            core.awaitLoadStarted();

            lc.shutdown();

            // Load still completes
            core.allowLoad();
            Anchor.Access<String, String> acc = get(a.future);
            assertNotNull(acc);
            assertEquals("value", acc.value());
            acc.close();
        }

        @Test
        void G8_closeTwiceAfterShutdown_noException() throws Exception {
            Future<Anchor.Access<String, String>> f = async(() -> lc.access("x"));
            core.allowLoad();
            Anchor.Access<String, String> a = get(f);

            lc.shutdown();
            a.close();
            a.close(); // idempotent — no exception
        }
    }

    // ---- Group H: Priority — delete beats access ----

    @Nested
    class H_Priority {

        @Test
        void H1_priorityOnLoadCompletion_deleteBeforeAccess() throws Exception {
            Async<Anchor.Access<String, String>> a =
                asyncTracked(() -> lc.access("x"));
            core.awaitLoadStarted();

            Future<String> b = async(() -> lc.access("x").value());
            Thread.sleep(30);
            Async<Void> c = asyncTrackedVoid(() -> lc.delete("x"));
            awaitBlocked(c.thread); // confirm delete is parked before releasing load

            // Load succeeds: delete wins over B
            core.allowLoad();
            core.awaitDeleteStarted(); // delete ran first, not B

            assertFalse(b.isDone()); // B is still parked

            core.allowDelete(); // delete done → B becomes loader
            core.awaitLoadStarted();
            core.allowLoad();

            assertEquals("value", get(b));
            get(c.future);
            assertEquals(1, core.deleteCalls.get());
        }

        @Test
        void H2_priorityOnSaveCompletion_deleteBeforeAccess() throws Exception {
            Future<Void> fA = asyncVoid(() -> {
                Anchor.Access<String, String> a = lc.access("x");
                a.save(); a.close();
            });
            core.allowLoad();
            core.awaitSaveStarted();

            Future<String> b = async(() -> lc.access("x").value());
            Thread.sleep(20);
            Future<Void> c = asyncVoid(() -> lc.delete("x"));
            Thread.sleep(30);

            // Save completes: delete wins
            core.allowSave();
            core.awaitDeleteStarted();
            assertFalse(b.isDone()); // B still parked

            core.allowDelete(); // delete done → B becomes loader
            core.awaitLoadStarted();
            core.allowLoad();

            assertEquals("value", get(b));
            get(fA); get(c);
            assertEquals(1, core.saveCalls.get());
            assertEquals(1, core.deleteCalls.get());
        }
    }

    // ---- Group I: Edge cases ----

    @Nested
    class I_EdgeCases {

        @Test
        void I1_closeTwice_idempotent() throws Exception {
            Future<Anchor.Access<String, String>> f = async(() -> lc.access("x"));
            core.allowLoad();
            Anchor.Access<String, String> a = get(f);
            a.close();
            a.close(); // second close is a no-op
            assertEquals(0, core.saveCalls.get());
        }

        @Test
        void I2_closeTwiceWithSave_onlyOneSave() throws Exception {
            Future<Anchor.Access<String, String>> f = async(() -> lc.access("x"));
            core.allowLoad();
            Anchor.Access<String, String> a = get(f);
            // Run on async thread since close triggers save (would block main thread on save gate)
            Future<Void> fClose = asyncVoid(() -> { a.save(); a.close(); a.close(); });
            core.awaitSaveStarted(); core.allowSave();
            get(fClose);
            assertEquals(1, core.saveCalls.get());
        }

        @Test
        void I3_shutdownIdempotent() {
            lc.shutdown();
            lc.shutdown(); // should not throw
        }

        @Test
        void I4_accessDeleteAccess_correctCounts() throws Exception {
            // access → close → delete → access
            Future<Void> f1 = asyncVoid(() -> lc.access("x").close());
            core.allowLoad();
            get(f1);

            Future<Void> f2 = asyncVoid(() -> lc.delete("x"));
            core.allowDelete();
            get(f2);

            Future<String> f3 = async(() -> lc.access("x").value());
            core.allowLoad();
            assertEquals("value", get(f3));

            assertEquals(2, core.loadCalls.get());
            assertEquals(1, core.deleteCalls.get());
            assertEquals(0, core.saveCalls.get());
        }

        @Test
        void I5_manyIds_independent() throws Exception {
            int n = 5;
            List<Future<String>> futures = new ArrayList<>();
            for (int i = 0; i < n; i++) {
                String id = "id-" + i;
                futures.add(async(() -> lc.access(id).value()));
            }
            for (int i = 0; i < n; i++) core.allowLoad();
            for (Future<String> f : futures) assertEquals("value", get(f));
            assertEquals(n, core.loadCalls.get());
        }
    }
}
