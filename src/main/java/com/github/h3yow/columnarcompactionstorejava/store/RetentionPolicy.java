package com.github.h3yow.columnarcompactionstorejava.store;

import java.util.ArrayList;
import java.util.List;

/**
 * Retention window for historical versions. A version is retained only when
 * it satisfies BOTH limits:
 * <ul>
 *   <li>it is among the most recent {@code maxVersions} versions, and</li>
 *   <li>it is not older than {@code maxAgeMillis}
 *       ({@link Long#MAX_VALUE} disables the age limit).</li>
 * </ul>
 *
 * <p>Because the two rules intersect, the retained set is always a suffix of
 * the oldest-first history: a version either stays fully reachable or its
 * catalog entry is dropped entirely — never half-retained.
 *
 * <p>Segments exclusively referenced by expired versions become reclaimable,
 * but never while an open snapshot pins them.
 */
public final class RetentionPolicy {

    public static final long AGE_DISABLED = Long.MAX_VALUE;

    private final long maxAgeMillis;
    private final int maxVersions;

    public RetentionPolicy(long maxAgeMillis, int maxVersions) {
        if (maxAgeMillis <= 0) {
            throw new IllegalArgumentException("maxAgeMillis must be positive");
        }
        if (maxVersions < 0) {
            throw new IllegalArgumentException("maxVersions must be >= 0");
        }
        this.maxAgeMillis = maxAgeMillis;
        this.maxVersions = maxVersions;
    }

    /** Default policy matching pre-snapshot behavior: only the current head. */
    public static RetentionPolicy currentOnly() {
        return new RetentionPolicy(AGE_DISABLED, 1);
    }

    /** Keep at most {@code n} most recent versions regardless of age. */
    public static RetentionPolicy keepVersions(int n) {
        return new RetentionPolicy(AGE_DISABLED, n);
    }

    /** Keep versions newer than the given age, and at most {@code n} of them. */
    public static RetentionPolicy ofAge(long maxAgeMillis, int maxVersions) {
        return new RetentionPolicy(maxAgeMillis, maxVersions);
    }

    public long maxAgeMillis() {
        return maxAgeMillis;
    }

    public int maxVersions() {
        return maxVersions;
    }

    /**
     * Returns the retained suffix of {@code history} (oldest first). Pure
     * function: callers decide what to do with the expired prefix. The head
     * (latest) version is always retained — it is the currently visible
     * state; retention governs how much history stays openable, never the
     * live table itself.
     */
    public List<VersionInfo> retainedVersions(List<VersionInfo> history, long nowMillis) {
        if (history.isEmpty()) {
            return List.of();
        }
        int ageCut = 0;
        if (maxAgeMillis != AGE_DISABLED) {
            long cutoff = nowMillis - maxAgeMillis;
            while (ageCut < history.size() - 1
                    && history.get(ageCut).createdAtMillis() < cutoff) {
                ageCut++;
            }
        }
        int countCut = Math.max(0, history.size() - maxVersions);
        // head is never expirable: cap the cut so the last version always stays
        int maxCut = history.size() - 1;
        int from = Math.min(Math.max(ageCut, countCut), maxCut);
        return new ArrayList<>(history.subList(from, history.size()));
    }
}
