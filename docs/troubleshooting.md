# Troubleshooting Guide

## Video Encoding Issues

### Symptom: Thousands of Tiny Video Segments

**Observed Behavior:**
```
Memory threshold reached, flushing video segment frames=9626 memory_mb=4 episode_index=0 segment_index=8626
Flushing video segment to temporary storage episode_index=0 segment_index=8626 cameras=3 total_frames=0
```

- `segment_index=8626` for only 9626 frames
- Many segments with `total_frames=0` or `total_frames=1`
- 9626 - 8626 = 1000 (first flush at frame 1000, then every frame triggers flush)

**Root Cause:**

The flush trigger uses cumulative frame count instead of images-since-last-flush:

```rust
// BUGGY: frame_data.len() is cumulative and never cleared
.should_flush(self.frame_data.len(), memory_bytes)
```

After `max_frames_per_chunk` frames (default 1000), every subsequent frame triggers a flush because `frame_data.len()` keeps growing.

**Solution:**

Track images added since last flush:

```rust
// In LerobotWriter struct:
image_count_since_flush: usize,

// In add_image():
self.image_buffers.entry(camera).or_default().push(data);
self.image_count_since_flush += 1;

// In flush_video_segment(), after clearing buffers:
self.image_count_since_flush = 0;

// In write_frame(), use image count for flush check:
if self.config.flushing.should_flush(self.image_count_since_flush, memory_bytes) {
    self.flush_video_segment()?;
}
```

---

### Symptom: Empty Segments Created (total_frames=0)

**Observed Behavior:**
```
Flushing video segment to temporary storage cameras=3 total_frames=0
Encoding videos with concurrent encoder cameras=3 total_frames=0
```

**Root Cause:**

1. Frame has only state/action data (no images)
2. Flush is triggered by frame count threshold
3. `image_buffers` is empty (cleared by previous flush, no new images added)

**Solution:**

The fix is already in place at `writer_impl.rs:710`:

```rust
fn flush_video_segment(&mut self) -> Result<()> {
    // Skip if no cameras have any frames
    if self.image_buffers.values().all(|v| v.is_empty()) {
        return Ok(());
    }
    // ... encoding logic
}
```

**Note:** Ensure the fix is deployed by rebuilding and redeploying the worker binary.

---

### Symptom: Memory Growing Despite Flushing

**Observed Behavior:**
- Memory usage continues to grow
- Eventually OOM on long recordings

**Root Cause:**

`frame_data` accumulates ALL frames (by design) for the final parquet write. This is expected - `frame_data` stores only state/action vectors which are relatively small compared to images.

**Solution:**

If memory is still an issue:
1. Reduce `max_frames_per_chunk` to flush more frequently
2. Reduce `max_memory_bytes` threshold
3. For very long recordings (>1M frames), consider chunked parquet output

---

## Pipeline Issues

### Symptom: Low FPS Output

**Observed Behavior:**
- Output has fewer frames than expected
- Frame timestamps seem incorrect

**Root Cause:**

Frame alignment uses `completion_window_ns` to wait for late messages. If the window is too short, messages may be dropped.

**Solution:**

Increase completion window in config:

```toml
[streaming]
fps = 30
completion_window_ns = 150000000  # 150ms (default is 3 frames)
```

---

### Symptom: Missing Camera Images

**Observed Behavior:**
- Some frames have no images
- Only state/action data in output

**Root Cause:**

Camera topics may publish at different rates than state topics. The frame alignment groups messages by timestamp, and if no camera message arrives within the completion window, the frame has no images.

**Solution:**

1. Check topic mappings in `lerobot_config.toml`
2. Verify camera message timestamps in source file
3. Increase `completion_window_ns` if cameras are slightly delayed

---

## Distributed Processing Issues

### Symptom: Episode Index Conflicts

**Observed Behavior:**
- Multiple workers writing to same episode directory
- Overwritten files

**Root Cause:**

Episode allocation not properly coordinated between workers.

**Solution:**

Ensure TiKV-based episode allocation is working:
1. Check TiKV connectivity
2. Verify `EpisodeAllocator` is used in worker
3. Check coordinator logs for allocation errors

---

### Symptom: Segments Not Merged

**Observed Behavior:**
- Multiple segment files remain in output
- No single MP4 per camera

**Root Cause:**

Finalizer not running or failing silently.

**Solution:**

1. Check finalizer logs for errors
2. Verify `merge_pending_segments()` is called in finalize
3. Check FFmpeg availability for segment concatenation

---

## Diagnostic Commands

### Check segment count for an episode:
```bash
find output/episode_000000 -name "segment_*.mp4" | wc -l
```

### Check segment sizes:
```bash
find output/episode_000000 -name "segment_*.mp4" -exec ls -la {} \;
```

### Verify parquet frame count:
```bash
python -c "import pyarrow.parquet as pq; print(len(pq.read_table('output/data/chunk-000/episode_000000.parquet')))"
```

### Check metadata:
```bash
cat output/meta/info.json | jq '.total_frames, .total_episodes'
```
