package keywhale.util.loader;

import java.util.function.Consumer;
import java.util.function.Function;

public interface Accessor<ID, VAL> {

    public void init(Access<ID, VAL> access);

    public void cancel();

    public default void onNotFound() {}

    public default boolean requiresSave() {
        return true;
    }

    public static <ID, VAL> Accessor<ID, VAL> of(Consumer<Access<ID, VAL>> consumer) {
        return of(
            (access) -> {
                consumer.accept(access);
                return null;
            },
            null,
            true
        );
    }

    public static <ID, VAL> Accessor<ID, VAL> of(
        Consumer<Access<ID, VAL>> consumer,
        Runnable onNotFound,
        boolean requiresSave
    ) {
        return of(
            (access) -> {
                consumer.accept(access);
                return null;
            },
            onNotFound,
            requiresSave
        );
    }

    public static <ID, VAL> Accessor<ID, VAL> of(Function<Access<ID, VAL>, Runnable> function) {
        return of(
            function,
            null,
            true
        );
    }

    public static <ID, VAL> Accessor<ID, VAL> of(
        Function<Access<ID, VAL>, Runnable> function,
        Runnable onNotFound,
        boolean requiresSave
    ) {
        return new Accessor<ID, VAL>() {

            private Runnable cancel;

            @Override
            public void init(Access<ID, VAL> access) {
                this.cancel = function.apply(access);
            }

            @Override
            public void cancel() {
                if (this.cancel != null) {
                    this.cancel.run();
                }
            }

            @Override
            public void onNotFound() {
                if (onNotFound != null) {
                    onNotFound.run();
                }
            }

            @Override
            public boolean requiresSave() {
                return requiresSave;
            }
            
        };
    }
}
