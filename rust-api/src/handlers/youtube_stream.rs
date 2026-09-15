// Progressive "stream YouTube audio while downloading" pipeline.
//
// The old lazy path ran `yt-dlp --extract-audio --audio-format mp3` and blocked on
// `.output().await` — the mp3 only existed once the whole download+transcode finished, so the
// first play of an un-cached video waited ~40s for byte 0. Here we run the pipeline ourselves:
//
//     yt-dlp -f bestaudio/best -o -  |  ffmpeg -i pipe:0 -vn -f mp3  {video_id}.mp3.partial
//
// ffmpeg writes the mp3 progressively, so the `.partial` file grows in near-real-time and can be
// tailed and served immediately. A DETACHED producer task owns the pipeline (a client disconnect
// does not abort it — the download still completes and caches), and one or more consumer responses
// tail the same growing file. On success the `.partial` is atomically renamed to the final mp3 and
// recorded exactly as the blocking path did; on failure it is deleted and nothing is recorded.
//
// ponytail: 150ms tail poll + probe loop instead of a fully event-driven reader — bounded extra
// latency, negligible for audio buffering; upgrade to purely Notify-driven if it ever matters.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::{Body, Bytes};
use axum::http::{header, StatusCode};
use axum::response::Response;
use futures::Stream;
use tokio::sync::Notify;
use tracing::{info, warn};

use crate::AppState;

// Serve the tail as soon as this many bytes of mp3 exist. Enough for a decoder to start; small
// enough that first audio lands in a couple of seconds on the happy path.
const FIRST_BYTES_THRESHOLD: u64 = 32 * 1024;
// How long we wait for the progressive pipeline to produce its first bytes before giving up and
// falling back to the old blocking download (handles fragmented/DASH formats that never pipe
// cleanly to stdout, so nothing is worse than today).
const PROBE_TIMEOUT: Duration = Duration::from_secs(20);
// Poll cadence for both the probe loop and the tailing reader while the producer is running.
const POLL: Duration = Duration::from_millis(150);
// Guard against a stuck producer holding a partial forever.
const MAX_DOWNLOAD: Duration = Duration::from_secs(30 * 60);
// Tail read buffer.
const READ_CHUNK: usize = 64 * 1024;

/// Shared state a producer exposes to its tailing consumers.
pub struct ProducerState {
    partial_path: PathBuf,
    final_path: PathBuf,
    done: AtomicBool,
    failed: AtomicBool,
    /// Woken as the partial grows and on done/failed transitions.
    notify: Notify,
    /// Signalled by a consumer that wants to abandon the progressive pipeline and fall back to a
    /// blocking download (empty-output timeout). `notify_one` so the producer never misses it.
    cancel: Notify,
}

lazy_static::lazy_static! {
    // video_id -> live producer. An entry exists only while a producer is running; it is removed on
    // completion (success or failure) so late arrivals hit the finished file via ServeFile instead.
    static ref PRODUCERS: Mutex<HashMap<String, Arc<ProducerState>>> = Mutex::new(HashMap::new());
}

/// What `begin_stream` decided the caller should do.
pub enum StreamStart {
    /// Progressive pipeline is producing; serve the tailing chunked body.
    Live(Arc<ProducerState>),
    /// The producer already finished during the probe; the cached mp3 is present — serve ServeFile.
    Cached,
    /// The progressive pipeline can't serve this video; fall back to the blocking download path.
    Fallback,
}

/// Start (or attach to) the producer for `video_id`, then wait briefly for progressive output.
/// The per-video `YT_DOWNLOAD_LOCKS` guard in `stream_episode` still serialises the "who becomes
/// the producer" decision; here the `PRODUCERS` map dedupes the actual pipeline so a second request
/// for the same video attaches as another tailer rather than starting a second download.
pub async fn begin_stream(
    state: &AppState,
    video_id: &str,
    user_id: i32,
    episode_id: i32,
) -> StreamStart {
    let producer = start_or_attach(state.clone(), video_id.to_string(), user_id, episode_id);

    let start = Instant::now();
    loop {
        if producer.failed.load(Ordering::SeqCst) {
            return StreamStart::Fallback;
        }
        if producer.done.load(Ordering::SeqCst) {
            // Finished (and renamed) before we saw any bytes — tiny/fast video. Serve the cache.
            return StreamStart::Cached;
        }
        if let Ok(md) = tokio::fs::metadata(&producer.partial_path).await {
            if md.len() >= FIRST_BYTES_THRESHOLD {
                return StreamStart::Live(producer);
            }
        }
        if start.elapsed() >= PROBE_TIMEOUT {
            // No progressive output in time — abandon this pipeline and let the caller do a
            // blocking download so nothing is worse than the old behaviour.
            warn!(
                "YouTube progressive pipeline produced no output for {} within {:?}; falling back",
                video_id, PROBE_TIMEOUT
            );
            producer.cancel.notify_one();
            return StreamStart::Fallback;
        }
        tokio::select! {
            _ = producer.notify.notified() => {}
            _ = tokio::time::sleep(POLL) => {}
        }
    }
}

/// Get-or-create the producer for `video_id`. The `PRODUCERS` mutex is held only for the
/// insert-if-absent (no await under the lock); the producer runs detached from any request.
fn start_or_attach(
    state: AppState,
    video_id: String,
    user_id: i32,
    episode_id: i32,
) -> Arc<ProducerState> {
    let mut map = PRODUCERS.lock().unwrap();
    if let Some(existing) = map.get(&video_id) {
        return existing.clone();
    }

    let base = format!("/opt/pinepods/downloads/youtube/{}.mp3", video_id);
    let producer = Arc::new(ProducerState {
        partial_path: PathBuf::from(format!("{}.partial", base)),
        final_path: PathBuf::from(base),
        done: AtomicBool::new(false),
        failed: AtomicBool::new(false),
        notify: Notify::new(),
        cancel: Notify::new(),
    });
    map.insert(video_id.clone(), producer.clone());
    drop(map);

    let task_producer = producer.clone();
    tokio::spawn(async move {
        run_producer(state, task_producer, video_id, user_id, episode_id).await;
    });

    producer
}

/// Detached producer: run the progressive pipeline, then rename+record on success or delete on
/// failure, and always remove the registry entry when done.
async fn run_producer(
    state: AppState,
    producer: Arc<ProducerState>,
    video_id: String,
    user_id: i32,
    episode_id: i32,
) {
    info!("Starting progressive YouTube pipeline for {}", video_id);

    let result = tokio::select! {
        r = tokio::time::timeout(MAX_DOWNLOAD, run_pipeline(&producer, &video_id)) => match r {
            Ok(inner) => inner,
            Err(_) => Err("max download time exceeded".to_string()),
        },
        // A consumer decided the progressive pipeline is a dud; drop the pipeline future (kills the
        // children via kill_on_drop) so its blocking fallback is the sole writer of the final file.
        _ = producer.cancel.notified() => Err("cancelled for blocking fallback".to_string()),
    };

    match result {
        Ok(_) => {
            if let Err(e) = tokio::fs::rename(&producer.partial_path, &producer.final_path).await {
                warn!("Failed to finalise {} mp3: {}", video_id, e);
                let _ = tokio::fs::remove_file(&producer.partial_path).await;
                producer.failed.store(true, Ordering::SeqCst);
            } else {
                let final_str = producer.final_path.to_string_lossy().to_string();
                // Duration + download record, mirroring the old blocking path.
                if let Some(duration) = crate::handlers::youtube::get_mp3_duration(&final_str) {
                    if let Err(e) = state
                        .db_pool
                        .update_youtube_video_duration(&video_id, duration)
                        .await
                    {
                        warn!("Failed to update duration for {}: {}", video_id, e);
                    }
                }
                if let Err(e) = state
                    .db_pool
                    .add_downloaded_video(user_id, episode_id, &final_str)
                    .await
                {
                    warn!("Failed to record DownloadedVideos for {}: {}", episode_id, e);
                }
                info!("Progressive YouTube pipeline completed for {}", video_id);
                producer.done.store(true, Ordering::SeqCst);
            }
        }
        Err(e) => {
            warn!("Progressive YouTube pipeline failed for {}: {}", video_id, e);
            let _ = tokio::fs::remove_file(&producer.partial_path).await;
            producer.failed.store(true, Ordering::SeqCst);
        }
    }

    // Wake any tailers so they observe the terminal flag, and drop the registry entry so new
    // requests take the cached ServeFile path.
    producer.notify.notify_waiters();
    PRODUCERS.lock().unwrap().remove(&video_id);
}

/// Run `yt-dlp -o - | ffmpeg -> {partial}` to completion. Both children are killed on drop, so
/// cancellation/timeout of the caller cleans them up. Returns the byte size on success.
async fn run_pipeline(producer: &ProducerState, video_id: &str) -> Result<u64, String> {
    use std::process::Stdio;
    use tokio::process::Command;

    let url = format!("https://www.youtube.com/watch?v={}", video_id);

    // yt-dlp streams the raw container to stdout as it downloads. Built via the shared helper so
    // YTDLP_PROXY is applied.
    let mut yt = crate::handlers::youtube::ytdlp_command()
        .args(["-f", "bestaudio/best", "--no-playlist", "--socket-timeout", "30", "-o", "-", &url])
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("spawn yt-dlp: {}", e))?;

    // ffmpeg transcodes the piped container to mp3, writing the partial progressively.
    let mut ff = Command::new("/usr/bin/ffmpeg")
        .args(["-loglevel", "error", "-i", "pipe:0", "-vn", "-f", "mp3", "-y"])
        .arg(&producer.partial_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| format!("spawn ffmpeg: {}", e))?;

    let mut yt_out = yt.stdout.take().ok_or("yt-dlp stdout missing")?;
    let mut ff_in = ff.stdin.take().ok_or("ffmpeg stdin missing")?;

    // Wire yt-dlp.stdout -> ffmpeg.stdin. Dropping ff_in at the end closes ffmpeg's stdin (EOF) so
    // it flushes and exits.
    let copy = tokio::spawn(async move {
        let r = tokio::io::copy(&mut yt_out, &mut ff_in).await;
        drop(ff_in);
        r
    });

    let yt_status = yt.wait().await.map_err(|e| format!("yt-dlp wait: {}", e))?;
    let ff_status = ff.wait().await.map_err(|e| format!("ffmpeg wait: {}", e))?;
    let copy_res = copy.await.map_err(|e| format!("copy join: {}", e))?;

    if !yt_status.success() {
        return Err(format!("yt-dlp exited {:?}", yt_status.code()));
    }
    if let Err(e) = copy_res {
        return Err(format!("pipe copy: {}", e));
    }
    if !ff_status.success() {
        return Err(format!("ffmpeg exited {:?}", ff_status.code()));
    }

    let size = tokio::fs::metadata(&producer.partial_path)
        .await
        .map(|m| m.len())
        .unwrap_or(0);
    if size == 0 {
        return Err("ffmpeg produced empty output".to_string());
    }
    Ok(size)
}

/// Build the live first-play response: 200, chunked (no Content-Length, no ranges), audio/mpeg,
/// body tailing the growing partial. Seeking is disabled for this first play; once cached the
/// normal range-capable ServeFile path takes over.
pub fn chunked_response(producer: Arc<ProducerState>) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "audio/mpeg")
        .header(header::ACCEPT_RANGES, "none")
        .header(header::CACHE_CONTROL, "no-store")
        .body(Body::from_stream(tail_stream(producer)))
        .expect("valid streaming response")
}

/// A stream that tails the growing partial (falling back to the final path if the producer renamed
/// it out from under an FD we hadn't opened yet). At EOF while the producer runs it waits for more;
/// when the producer is done it drains the remainder and finishes; on failure it ends.
fn tail_stream(producer: Arc<ProducerState>) -> impl Stream<Item = Result<Bytes, std::io::Error>> {
    struct TailState {
        producer: Arc<ProducerState>,
        file: Option<tokio::fs::File>,
    }

    futures::stream::unfold(
        TailState { producer, file: None },
        |mut st| async move {
            use tokio::io::AsyncReadExt;

            loop {
                if st.file.is_none() {
                    let opened = match tokio::fs::File::open(&st.producer.partial_path).await {
                        Ok(f) => Some(f),
                        Err(_) => tokio::fs::File::open(&st.producer.final_path).await.ok(),
                    };
                    match opened {
                        Some(f) => st.file = Some(f),
                        None => {
                            if st.producer.failed.load(Ordering::SeqCst) {
                                return None;
                            }
                            wait_for_progress(&st.producer).await;
                            continue;
                        }
                    }
                }

                let file = st.file.as_mut().unwrap();
                let mut buf = vec![0u8; READ_CHUNK];
                match file.read(&mut buf).await {
                    Ok(0) => {
                        // Caught up to the current end of file.
                        if st.producer.done.load(Ordering::SeqCst)
                            || st.producer.failed.load(Ordering::SeqCst)
                        {
                            return None;
                        }
                        wait_for_progress(&st.producer).await;
                        continue;
                    }
                    Ok(n) => {
                        buf.truncate(n);
                        return Some((Ok(Bytes::from(buf)), st));
                    }
                    Err(e) => return Some((Err(e), st)),
                }
            }
        },
    )
}

/// Wait for the producer to make progress (or time-slice), so the tailer doesn't busy-spin.
async fn wait_for_progress(producer: &ProducerState) {
    tokio::select! {
        _ = producer.notify.notified() => {}
        _ = tokio::time::sleep(POLL) => {}
    }
}
