// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <algorithm>
#include <cstddef>
#include <string>

#include "common/constexpr.h"
#include "fmt/format.h"

namespace starrocks::pipeline {

// Cost-model-based compaction decision framework.
// Decides whether to compact (merge) small/sparse chunks into dense chunks
// based on runtime statistics, following the SIGMOD 2025 paper
// "Data Chunk Compaction in Vectorized Execution".
class ChunkCompactionPolicy {
public:
    struct CompactionDecision {
        bool should_compact = true;
        size_t target_chunk_size = DEFAULT_CHUNK_SIZE;
        std::string reason;
    };

    // Evaluate whether compaction is beneficial given runtime statistics.
    //
    // @param selectivity         Fraction of rows passing filter (0.0 - 1.0)
    // @param downstream_op_count Number of downstream operators that will consume the chunk
    // @param avg_row_bytes       Average bytes per row across all columns
    // @param current_chunk_rows  Number of rows in the current (sparse) chunk
    // @param target_chunk_size   Desired target chunk size (e.g. 4096)
    static CompactionDecision evaluate(double selectivity, size_t downstream_op_count, size_t avg_row_bytes,
                                       size_t current_chunk_rows, size_t target_chunk_size) {
        CompactionDecision decision;
        decision.target_chunk_size = compute_adaptive_target(avg_row_bytes, target_chunk_size);

        // If chunk is already nearly full, skip compaction
        if (current_chunk_rows >= target_chunk_size * HIGH_WATERMARK_RATE) {
            decision.should_compact = false;
            decision.reason = "chunk already dense";
            return decision;
        }

        // Cost-benefit analysis (simplified from SIGMOD 2025 paper):
        //   Benefit = saved per-tuple overhead * downstream_ops * empty_slots
        //   Cost    = copy cost per byte * row_bytes * current_rows
        double empty_slots = static_cast<double>(target_chunk_size - current_chunk_rows);
        double benefit = empty_slots * downstream_op_count * PER_TUPLE_OVERHEAD_NS;
        double cost = static_cast<double>(avg_row_bytes) * current_chunk_rows * COPY_COST_PER_BYTE_NS;

        decision.should_compact = (benefit > cost * SAFETY_FACTOR);
        decision.reason = fmt::format("benefit={:.0f} cost={:.0f} ratio={:.2f}", benefit, cost,
                                      cost > 0 ? benefit / cost : 999.0);
        return decision;
    }

    // Compute adaptive target chunk size based on row width (Velox-style byte budget).
    // Wider rows → smaller batch to fit in L2 cache.
    // Narrower rows → larger batch to amortize per-chunk overhead.
    static size_t compute_adaptive_target(size_t avg_row_bytes, size_t default_target = DEFAULT_CHUNK_SIZE) {
        if (avg_row_bytes == 0) {
            return default_target;
        }
        size_t rows = TARGET_BATCH_BYTES / avg_row_bytes;
        return std::clamp(rows, MIN_ADAPTIVE_CHUNK_SIZE, MAX_ADAPTIVE_CHUNK_SIZE);
    }

    // Threshold for "big batch passthrough" — chunks larger than this
    // are passed through without buffering (Arrow-rs BatchCoalescer style).
    static size_t biggest_coalesce_threshold(size_t target_chunk_size) { return target_chunk_size / 2; }

private:
    // Tuning constants (can be made configurable via session variables later)

    // Per-tuple overhead in nanoseconds (function call, scheduling, memory alloc)
    static constexpr double PER_TUPLE_OVERHEAD_NS = 5.0;

    // Cost of copying one byte during compaction (nanoseconds)
    static constexpr double COPY_COST_PER_BYTE_NS = 0.3;

    // Safety factor: compaction must be this many times more beneficial than costly
    static constexpr double SAFETY_FACTOR = 1.5;

    // Target batch bytes for adaptive sizing (~256KB, fits L2 cache)
    static constexpr size_t TARGET_BATCH_BYTES = 256 * 1024;

    // Adaptive chunk size bounds
    static constexpr size_t MIN_ADAPTIVE_CHUNK_SIZE = 256;
    static constexpr size_t MAX_ADAPTIVE_CHUNK_SIZE = 8192;

    // High watermark: if chunk is >= this fraction of target, don't compact
    static constexpr double HIGH_WATERMARK_RATE = 0.85;
};

} // namespace starrocks::pipeline
