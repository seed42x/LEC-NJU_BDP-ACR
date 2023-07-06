package utils;

import java.util.Objects;

public final class Pair<F, S> {
    private final F fst;
    private final S snd;

    public Pair(F fst, S snd) {
        this.fst = fst;
        this.snd = snd;
    }

    public static <F, S> Pair<F, S> of(F fst, S snd) {
        return new Pair<>(fst, snd);
    }

    public F getFst() {
        return fst;
    }

    public S getSnd() {
        return snd;
    }

    public static boolean doubleEquals(Double lhs, Double rhs) {
        return Math.abs(lhs - rhs) < 1e-3;
    }

    public static boolean generalEquals(Object lhs, Object rhs) {
        if (lhs instanceof Double && rhs instanceof Double) {
            return doubleEquals((Double) lhs, (Double) rhs);
        }
        return Objects.equals(lhs, rhs);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Pair<?, ?> pair = (Pair<?, ?>) o;
        return generalEquals(fst, pair.fst) && generalEquals(snd, pair.snd);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fst, snd);
    }
}
