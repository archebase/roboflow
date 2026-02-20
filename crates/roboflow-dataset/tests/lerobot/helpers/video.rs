// SPDX-FileCopyrightText: 2026 ArcheBase
//
// SPDX-License-Identifier: MulanPSL-2.0

use std::path::Path;
use std::process::Command;

#[derive(Debug, Clone)]
pub struct VideoProperties {
    pub width: u32,
    pub height: u32,
    pub nb_frames: u64,
    pub codec_name: String,
    pub fps: f64,
}

fn ffprobe_path() -> Option<&'static str> {
    if Command::new("ffprobe")
        .arg("-version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
    {
        Some("ffprobe")
    } else {
        None
    }
}

pub fn probe_video_properties(path: &Path) -> Option<VideoProperties> {
    let ffprobe = ffprobe_path()?;

    let output = Command::new(ffprobe)
        .arg("-v")
        .arg("error")
        .arg("-show_streams")
        .arg("-of")
        .arg("json")
        .arg("-select_streams")
        .arg("v:0")
        .arg(path)
        .output()
        .ok()?;

    let json_str = String::from_utf8(output.stdout).ok()?;
    let json: serde_json::Value = serde_json::from_str(&json_str).ok()?;

    let streams = json.get("streams")?.as_array()?;
    let stream = streams.first()?;

    let width = stream.get("width")?.as_u64()? as u32;
    let height = stream.get("height")?.as_u64()? as u32;

    let nb_frames = stream
        .get("nb_read_frames")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    let codec_name = stream
        .get("codec_name")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    let fps = stream
        .get("r_frame_rate")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0);

    Some(VideoProperties {
        width,
        height,
        nb_frames,
        codec_name,
        fps,
    })
}

pub fn ffprobe_available() -> bool {
    ffprobe_path().is_some()
}
