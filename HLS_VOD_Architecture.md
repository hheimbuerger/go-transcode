# HLS VOD Architecture (go-transcode)

> [!WARNING]
> This document is work in progress!

This document describes how `go-transcode` performs HTTP Live Streaming (HLS) for Video-on-Demand (VOD). It details the end-to-end workflow, segmenting, buffering, transcoding, and tuning. All content is specific to VOD: there are no live HLS behaviors described.

## 1. End-to-End VOD Workflow

go-transcode runs an HTTP server that serves HLS manifests (playlists) and media segments on demand. The workflow is divided into three main phases:

- **System Startup:** On startup, the server loads configuration, scans media files, and prepares segment boundaries (based on `SegmentLength` and `SegmentOffset`, often aligning with keyframes). No transcoding occurs at this point.
- **Background State:** The server remains idle, waiting for HTTP GET requests from clients (players) for manifests or segments. No pre-transcoding or background processing is performed unless triggered by a request.
- **On-Demand Response:**
    - **Playlist Request:** When a client requests a playlist (e.g., `/vod/video.mp4/index.m3u8`), the server generates and serves a static `.m3u8` manifest listing all segments.
    - **Segment Request:** When a client requests a segment (`segment-0.ts`, etc.), the server infers the playhead from the segment number. If the segment is not yet transcoded, the server triggers FFmpeg to transcode it and, if needed, a batch of future segments (using the prefetching/caching mechanism described by `SegmentBufferMin` and `SegmentBufferMax`).
    - **Streaming:** As soon as FFmpeg produces bytes for a segment, they are streamed directly to the client using progressive streaming. The player can start playback before the segment is fully written to disk.

### Progressive Streaming Mechanism

go-transcode implements true progressive streaming by connecting FFmpeg's standard output directly to the HTTP response using Go's `io.Pipe`. When a segment is requested:

1. FFmpeg writes segment data to its stdout as it processes the video
2. The server reads from this pipe in real-time and writes each chunk directly to the HTTP response
3. Data is flushed immediately to the client without buffering
4. The client receives and can begin processing segment data while FFmpeg is still encoding

This design minimizes latency - if FFmpeg is 10% done with a segment, the client already has those first bytes and can start buffering. The HTTP request completes as soon as FFmpeg finishes, without waiting for disk I/O.

**Diagram:**
```
… █ █ █ ▶︎ ▒ ▒ ▒ ▒ ▒ …
      ^        ^^^^^
      |        |_____ up to SegmentBufferMax segments transcoded in one batch
      |
      |______________ at least SegmentBufferMin segments must exist beyond play-head
```

## 2. Configuration Parameters

This section covers the four key parameters that control segmentation, buffering, and transcoding. Tuning these parameters allows you to balance startup latency, playback smoothness, and resource usage.

| Parameter           | Description                                        |
|---------------------|----------------------------------------------------|
| `SegmentLength`     | Target segment length (seconds). Shorter = faster startup, more segments, higher FFmpeg overhead. |
| `SegmentOffset`     | Permitted deviation from target (seconds) for aligning with keyframes. |
| `SegmentBufferMin`  | Minimum number of future segments to buffer ahead of the playhead. |
| `SegmentBufferMax`  | Maximum number of segments transcoded in one FFmpeg batch. |

### System Configuration

All flags can either be placed under the `vod:` section of `config.yaml` or passed as CLI flags.

| YAML Key                | CLI flag                    | Default |
|-------------------------|-----------------------------|---------|
| `vod.segment-length`    | `--vod-segment-length`      | `4`     |
| `vod.segment-offset`    | `--vod-segment-offset`      | `1`     |
| `vod.segment-buffer-min`| `--vod-segment-buffer-min`  | `3`     |
| `vod.segment-buffer-max`| `--vod-segment-buffer-max`  | `5`     |
| `vod.ready-timeout`     | `--vod-ready-timeout`       | `80`    |
| `vod.transcode-timeout` | `--vod-transcode-timeout`   | `10`    |

## 3. Buffering and Transcoding Workflow

The transcoder maintains a sliding window of buffered segments ahead of the inferred playhead (the most recently requested segment). When a segment request arrives:

1. The system checks how many segments are already transcoded or queued beyond the playhead.
2. If fewer than `SegmentBufferMin` are available, it triggers a new FFmpeg batch to transcode up to `SegmentBufferMax` future segments.
3. Segments are streamed to the client as soon as they are produced by FFmpeg, enabling progressive playback.

The buffering and transcoding workflow begins by determining segment boundaries, which are chosen based on the configured `SegmentLength` and allowed deviation (`SegmentOffset`), often aligning with keyframes for optimal performance. When a client requests a segment, the system infers the current playhead and checks how many future segments are already available (either transcoded or in-progress).

### Prefetching and Batching

Rather than transcoding each requested segment just-in-time, the system employs a *sliding-window batching strategy*:

* If the number of **ready or in-progress** segments **ahead of the current play-head** is **< `SegmentBufferMin`**, the server launches a new FFmpeg process to transcode **the first contiguous hole** (missing range) directly after the play-head, but **never more than `SegmentBufferMax` segments**.
* `SegmentBufferMin` is always evaluated **relative to the most recent request**. When the player seeks, the window is recalculated and any gaps that now fall **behind** the new play-head are simply ignored. They will only be transcoded if they are requested again later.

This behaviour prevents the server from wasting CPU/GPU time on segments the client is unlikely to watch while still maintaining a safety cushion for smooth playback. Batching amortises FFmpeg start-up overhead, and limiting the batch to `SegmentBufferMax` keeps latency low after abrupt seeks.

### Monitoring

To avoid redundant work, the system maintains two per-segment maps: one tracking which segments are already transcoded and another for segments currently being transcoded (in-progress). When launching a new FFmpeg batch, only segments that are neither transcoded nor in-progress are included. This ensures that, under normal conditions, each FFmpeg process works on a unique (typically disjoint) set of segments, even if multiple batches are running in parallel (such as after seeks). Rare race conditions—such as a process crash and immediate retry—may result in temporary overlap, but this is resolved as state is reconciled.

### Failure recovery

Failure detection and recovery are handled internally: the system monitors the FFmpeg process exit status and parses its output channels to determine which segments were successfully produced. If a batch fails or does not produce all expected segments, the system clears the in-progress flags for those segments immediately upon process termination—there is no separate watchdog or filesystem polling. On subsequent requests, any missing or failed segments become eligible for re-transcoding. There is no automatic background retry; instead, recovery is demand-driven, triggered by future client requests. The system does not maintain a global registry of FFmpeg processes; per-segment in-progress flags are sufficient to coordinate concurrent work and prevent duplication in practice.

## 4. Tuning Tips

- **Startup latency** is reduced by lowering `SegmentLength`, but this increases transcoding frequency and network overhead.
- **Buffering**: `SegmentBufferMin` defines a *sliding window* in front of the play-head. If the window shrinks below the threshold, the server queues a batch (up to `SegmentBufferMax`) of the first contiguous missing segments **after** the play-head. Segments that end up **behind** the play-head after a seek are *abandoned* and will only be transcoded if requested again. Consequently, high `SegmentBufferMin` values do **not** cause unnecessary transcoding load.
- **Batching**: `SegmentBufferMax` controls how many segments are processed per FFmpeg invocation. Batching reduces FFmpeg startup time, but large batches block the FFmpeg process, preventing it from being re-tasked for new requests (e.g., after seeks) until the batch completes.
- **Keyframe alignment**: `SegmentOffset` allows flexibility to align segment boundaries with keyframes, avoiding expensive re-encoding.
- For low-latency startup, use a lower `SegmentLength` and moderate `SegmentBufferMin`.
- For efficient resource use, set `SegmentBufferMax` equal to or slightly above `SegmentBufferMin`.
- Use `SegmentOffset` ≈ half the GOP duration for best keyframe reuse.
- On systems with NVIDIA GPUs, GeForce cards allow up to 3 concurrent NVENC sessions; professional cards allow more. Hitting this limit can increase latency.

## 5. Error Handling & Troubleshooting

The system implements comprehensive error detection and recovery mechanisms:

### Common Error Scenarios

- **Segment not found (404)**: Occurs when a segment index doesn't exist in the segments map, typically due to seeking beyond media boundaries
- **File missing on disk (404)**: Segment is marked as transcoded but the file doesn't exist, indicating transcoding failure or cleanup issues  
- **Transcoding timeout (504)**: FFmpeg process doesn't complete within the configured `TranscodeTimeout` period
- **FFmpeg process failure (500)**: FFmpeg exits with error code, often due to corrupted input or resource constraints
- **Client disconnection**: Detected via request context cancellation, allows cleanup of in-progress work

### Recovery Behavior

- Failed segments are immediately marked as available for re-transcoding
- No automatic background retry - recovery is demand-driven by future requests
- Process failures clear in-progress flags, allowing subsequent requests to retry
- Client timeouts are handled gracefully without affecting other concurrent requests

## 6. System Limitations

### FFmpeg Batch Processing

When a user seeks to a new position while an FFmpeg batch is already processing segments, the current batch is **not** interrupted. The system waits for the current batch to complete before starting transcoding for the newly requested segments. This can introduce additional latency during seeks, especially with larger `SegmentBufferMax` values.

### Resource Constraints

- **NVENC Sessions**: NVIDIA GeForce cards support maximum 3 concurrent NVENC sessions; professional cards allow more
- **Memory Usage**: Each FFmpeg process consumes memory proportional to video resolution and batch size
- **Disk I/O**: Concurrent transcoding can create I/O bottlenecks, especially on traditional hard drives
- **CPU Cores**: FFmpeg uses multiple threads but batch processing is sequential within each process