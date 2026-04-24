# Anchor

Thread-safe lifecycle manager for keyed in-memory values. Coordinates concurrent access, lazy loading, write-back saving, and deletion — without holding a lock during I/O.

---

## Core Concepts

### Value Lifecycle

Each keyed value moves through states internally:

- **Unknown** — not in memory, no tracker
- **Loading** — being fetched from storage
- **Active** — in memory, held by one or more accessors
- **Saving** — being written back to storage
- **Deleting** — being deleted from storage

Requests that arrive during a transition are queued and handled automatically once it completes. Delete requests take priority over pending accesses.

### Threading Model

`Anchor` never schedules work. The thread that calls `access(...)` or `delete(...)` is the thread that performs the I/O — block it with your own executor if needed. Multiple callers for the same key coalesce: only one performs the I/O, the rest park until it finishes.

---

## Step 1: Implement `Core`

`Core<ID, VAL>` is the storage interface. Override the methods you need.

```java
public class PlayerCore implements Anchor.Core<UUID, PlayerData> {

    private final Database database;

    public PlayerCore(Database database) {
        this.database = database;
    }

    @Override
    public PlayerData load(UUID id) {
        PlayerData data = database.load(id);
        if (data == null) throw new Core.NotFoundException();
        return data;
    }

    @Override
    public void save(UUID id, PlayerData data) {
        database.save(id, data);
    }

    @Override
    public void delete(UUID id) {
        database.delete(id);
    }
}
```

Throw `Core.NotFoundException` from `load` if the value does not exist. Any `RuntimeException` thrown from `load`, `save`, or `delete` propagates to the caller.

---

## Step 2: Access Values

```java
Anchor<UUID, PlayerData> anchor = new Anchor<>(new PlayerCore(database));

try (Anchor.Access<UUID, PlayerData> access = anchor.access(playerId)) {
    access.value().setCoins(access.value().getCoins() + 100);
    access.save(); // marks value as modified; triggers a save on close
} // access.close() called here
```

`access(ID)` blocks until the value is loaded and the access is provisioned. Each access has a cancellation `Event` (retrieved via `getCancelEvent()`) that is set when a delete request arrives — your code should watch it and release the access early.

Closing the last access on a value triggers a save if `save()` was called, then evicts the value from memory.

### `Access<ID, VAL>`

| Method | Description |
|---|---|
| `id()` | The key |
| `value()` | The loaded value |
| `save()` | Marks value as modified; a save will run when all accessors close |
| `getCancelEvent()` | Returns an `Event` that is set when a delete is requested for this key |
| `close()` | Releases the access |

---

## Step 3: Delete Values

```java
anchor.delete(playerId);
```

`delete(ID)` blocks until the delete is complete. If any accessors are currently active, their cancellation `Event`s are set — your code should release those accesses so the delete can proceed. Once all accessors close, the delete runs.

---

## Shutdown

```java
anchor.shutdown();
```

Stops accepting new requests (`ShuttingDownException` is thrown for any subsequent `access` or `delete` calls). All active accessors have their cancellation events set. In-flight I/O (currently inside `load`, `save`, or `delete`) is not interrupted.

---

## Synchronizer

A try-with-resources mutex with optional per-key locking. Independent keys are locked separately; the default overload uses a shared internal key.

```java
Synchronizer sync = new Synchronizer();

// Default lock
try (Synchronizer.Lock lock = sync.lock()) {
    // exclusive access
}

// Keyed lock — independent from other keys
try (Synchronizer.Lock lock = sync.lock(someKey)) {
    // exclusive access for this key
}
```

Locks are reentrant. Entries are cleaned up automatically when no threads hold or are waiting on them.
