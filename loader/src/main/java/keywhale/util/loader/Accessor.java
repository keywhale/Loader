package keywhale.util.loader;

import java.util.function.Consumer;
import java.util.function.Function;

public interface Accessor<ID, VAL> {

    public void init(Access<ID, VAL> access);

    public void cancel();

    public default void onNotFound() {}

    public static <ID, VAL> Accessor<ID, VAL> of(Consumer<Access<ID, VAL>> consumer) {
        return of(
            (access) -> {
                consumer.accept(access);
                return null;
            },
            null
        );
    }

    public static <ID, VAL> Accessor<ID, VAL> of(
        Consumer<Access<ID, VAL>> consumer,
        Runnable onNotFound
    ) {
        return of(
            (access) -> {
                consumer.accept(access);
                return null;
            },
            onNotFound
        );
    }

    public static <ID, VAL> Accessor<ID, VAL> of(Function<Access<ID, VAL>, Runnable> function) {
        return of(
            function,
            null
        );
    }

    public static <ID, VAL> Accessor<ID, VAL> of(
        Function<Access<ID, VAL>, Runnable> function,
        Runnable onNotFound
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
            
        };
    }
}
