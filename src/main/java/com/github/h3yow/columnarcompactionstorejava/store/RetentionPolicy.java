package com.github.h3yow.columnarcompactionstorejava.store;

import java.time.Duration;

/**
 * Retention policy deciding which live segments are expired and may be
 * reclaimed by {@link ColumnarStore#enforceRetention(RetentionPolicy)}.
 *
 * <p>Expiration is decided at segment granularity against the segment's
 * creation timestamp / position:
 * <ul>
 *   <li>{@link #maxAge(Duration)} — segments created more than the given
 *       span ago are expired.</li>
 *   <li>{@link #keepLastVersions(int)} — all live segments except the newest
 *       {@code k} (in creation order) are expired.</li>
 * </ul>
 */
public sealed interface RetentionPolicy {

    /** Expire segments whose creation timestamp is older than {@code maxAge}. */
    record MaxAge(Duration maxAge) implements RetentionPolicy {
        public MaxAge {
            if (maxAge == null || maxAge.isNegative()) {
                throw new StoreException(ErrorCode.MALFORMED_BATCH,
                        "maxAge must be a non-negative duration");
            }
        }
    }

    /** Keep only the newest {@code keep} live segments; older ones expire. */
    record KeepLastVersions(int keep) implements RetentionPolicy {
        public KeepLastVersions {
            if (keep < 0) {
                throw new StoreException(ErrorCode.MALFORMED_BATCH,
                        "keep must be >= 0");
            }
        }
    }

    static RetentionPolicy maxAge(Duration maxAge) {
        return new MaxAge(maxAge);
    }

    static RetentionPolicy keepLastVersions(int keep) {
        return new KeepLastVersions(keep);
    }
}
