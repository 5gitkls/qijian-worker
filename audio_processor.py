"""
MelodAI Python Audio Processor
================================
音频后处理服务，使用 moviepy 2.2.1 对 ACE-Step 1.5 生成的原始音频进行剪辑、
淡入淡出、格式转换、波形图生成等操作。

依赖版本:
  - moviepy==2.2.1
  - numpy>=1.24
  - redis>=5.0
  - requests>=2.31
  - boto3>=1.34  (OSS/S3上传)
"""

import os
import json
import time
import hmac
import hashlib
import logging
import shutil
import tempfile
import traceback
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse, urlencode

import numpy as np
import redis
import requests
import boto3
from botocore.config import Config

# moviepy 2.2.1 API
from moviepy import AudioFileClip, concatenate_audioclips, CompositeAudioClip
from moviepy.audio.fx import AudioFadeIn, AudioFadeOut, MultiplyVolume

# ─────────────────────────────────────────────
# 配置
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("melodai.audio_processor")

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")          # 生产必须设置密码
REDIS_DB = int(os.getenv("REDIS_DB", 0))

QUEUE_KEY = "melodai:audio_queue"                      # Redis Stream key（与 qijian-infer 写入的队列一致）
CONSUMER_GROUP = "qijian-worker-group"
CONSUMER_NAME = f"worker_{os.getpid()}"
PENDING_IDLE_MS = int(os.getenv("PENDING_IDLE_MS", "60000"))
PENDING_RETRY_COUNT = int(os.getenv("PENDING_RETRY_COUNT", "20"))

ACE_STEP_BASE_URL = os.getenv("ACE_STEP_BASE_URL", "http://127.0.0.1:8000")
# qijian-api 默认端口为 80（application.yml: server.port=80）
# 生产环境通过环境变量 JAVA_CALLBACK_URL 覆盖
JAVA_CALLBACK_URL = os.getenv("JAVA_CALLBACK_URL", "http://127.0.0.1:80/api/music/callback")
JAVA_CALLBACK_SECRET = os.getenv("JAVA_CALLBACK_SECRET", os.getenv("MUSIC_INTERNAL_CALLBACK_SECRET", ""))

# OSS / S3 配置
OSS_ENDPOINT = os.getenv("OSS_ENDPOINT", "")
OSS_BUCKET = os.getenv("OSS_BUCKET", "melodai-music")
OSS_ACCESS_KEY = os.getenv("OSS_ACCESS_KEY", "")
OSS_SECRET_KEY = os.getenv("OSS_SECRET_KEY", "")
OSS_CDN_PREFIX = os.getenv("OSS_CDN_PREFIX", "https://cdn.melodai.com")
LOCAL_STORAGE_DIR = os.getenv("LOCAL_STORAGE_DIR", "").strip()
LOCAL_STORAGE_URL_PREFIX = os.getenv("LOCAL_STORAGE_URL_PREFIX", "http://127.0.0.1:18080/local").rstrip("/")

# 音频处理参数
DEFAULT_FADE_IN_SEC = 1.5          # 淡入时长（秒）
DEFAULT_FADE_OUT_SEC = 3.0         # 淡出时长（秒）
DEFAULT_MAX_DURATION = 300         # 最大时长（秒），超出则截断
OUTPUT_FORMAT = "mp3"              # 输出格式
OUTPUT_BITRATE = "192k"            # 输出码率
TEMP_DIR = Path(tempfile.gettempdir()) / "melodai_audio"
TEMP_DIR.mkdir(parents=True, exist_ok=True)


# ─────────────────────────────────────────────
# Redis 连接
# ─────────────────────────────────────────────
def get_redis_client() -> redis.Redis:
    """创建 Redis 连接，强制要求密码（生产环境）。"""
    kwargs: dict = {
        "host": REDIS_HOST,
        "port": REDIS_PORT,
        "db": REDIS_DB,
        "decode_responses": True,
        "socket_connect_timeout": 5,
        "socket_timeout": 10,
        "retry_on_timeout": True,
    }
    if REDIS_PASSWORD:
        kwargs["password"] = REDIS_PASSWORD
    else:
        logger.warning("REDIS_PASSWORD 未设置，生产环境存在安全风险！")
    return redis.Redis(**kwargs)


# ─────────────────────────────────────────────
# OSS 上传
# ─────────────────────────────────────────────
def upload_to_oss(local_path: Path, object_key: str) -> str:
    """
    上传文件到 OSS/S3，返回 CDN 访问 URL。

    当设置 LOCAL_STORAGE_DIR 时启用本地文件存储回退，主要用于本地联调、CI 或无云存储凭据的测试环境。
    生产环境应配置 OSS_ENDPOINT/OSS_ACCESS_KEY/OSS_SECRET_KEY，并关闭 LOCAL_STORAGE_DIR。
    """
    if LOCAL_STORAGE_DIR:
        storage_root = Path(LOCAL_STORAGE_DIR)
        target_path = storage_root / object_key
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, target_path)
        local_url = f"{LOCAL_STORAGE_URL_PREFIX}/{object_key}"
        logger.info("已保存至本地存储: %s", local_url)
        return local_url

    if not OSS_ACCESS_KEY or not OSS_SECRET_KEY:
        raise RuntimeError(
            "OSS/S3 凭据未配置，无法上传音频；本地联调可设置 LOCAL_STORAGE_DIR 启用本地存储回退"
        )

    s3 = boto3.client(
        "s3",
        endpoint_url=OSS_ENDPOINT if OSS_ENDPOINT else None,
        aws_access_key_id=OSS_ACCESS_KEY,
        aws_secret_access_key=OSS_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )
    with open(local_path, "rb") as f:
        s3.put_object(
            Bucket=OSS_BUCKET,
            Key=object_key,
            Body=f,
            ContentType="audio/mpeg",
            CacheControl="max-age=31536000",
        )
    cdn_url = f"{OSS_CDN_PREFIX.rstrip('/')}/{object_key}"
    logger.info(f"已上传至 OSS: {cdn_url}")
    return cdn_url


# ─────────────────────────────────────────────
# 核心音频处理函数（moviepy 2.2.1）
# ─────────────────────────────────────────────
def process_audio(
    input_path: Path,
    output_path: Path,
    fade_in: float = DEFAULT_FADE_IN_SEC,
    fade_out: float = DEFAULT_FADE_OUT_SEC,
    max_duration: float = DEFAULT_MAX_DURATION,
    volume: float = 1.0,
    trim_start: float = 0.0,
    trim_end: Optional[float] = None,
) -> dict:
    """
    对原始音频进行后处理：
      1. 截取指定片段（trim_start ~ trim_end）
      2. 限制最大时长
      3. 调整音量
      4. 添加淡入淡出
      5. 导出为 MP3

    moviepy 2.2.1 变更说明：
      - AudioFadeIn / AudioFadeOut 作为 fx 对象使用（.with_effects([...])）
      - MultiplyVolume 替代旧版 volumex()
      - 不再支持 write_audiofile 的 verbose 参数，改用 logger

    Returns:
        dict: { "duration": float, "output_path": str }
    """
    logger.info(f"开始处理音频: {input_path}")

    with AudioFileClip(str(input_path)) as clip:
        # 1. 截取片段
        start = max(0.0, trim_start)
        end = trim_end if trim_end is not None else clip.duration
        end = min(end, clip.duration)
        if start >= end:
            raise ValueError(f"无效的截取范围: start={start}, end={end}")

        clip = clip.subclipped(start, end)

        # 2. 限制最大时长
        if clip.duration > max_duration:
            logger.info(f"音频超过最大时长 {max_duration}s，截断至 {max_duration}s")
            clip = clip.subclipped(0, max_duration)

        # 3. 调整音量 + 淡入淡出（moviepy 2.2.1 fx API）
        effects = []
        if volume != 1.0:
            effects.append(MultiplyVolume(volume))
        if fade_in > 0:
            effects.append(AudioFadeIn(fade_in))
        if fade_out > 0 and clip.duration > fade_out:
            effects.append(AudioFadeOut(fade_out))

        if effects:
            clip = clip.with_effects(effects)

        # 4. 导出
        clip.write_audiofile(
            str(output_path),
            fps=44100,
            bitrate=OUTPUT_BITRATE,
            codec="libmp3lame",
            logger=None,          # 避免 moviepy 2.2.1 的 verbose 参数问题
        )

        final_duration = clip.duration

    logger.info(f"音频处理完成: {output_path}, 时长: {final_duration:.2f}s")
    return {"duration": final_duration, "output_path": str(output_path)}


def merge_audio_clips(
    clip_paths: list[Path],
    output_path: Path,
    crossfade_sec: float = 1.0,
) -> dict:
    """
    将多个音频片段合并（带交叉淡化）。
    用于将 ACE-Step 生成的多段音频拼接成完整歌曲。

    moviepy 2.2.x: 当前依赖版本的 concatenate_audioclips 不支持 method 参数，采用顺序拼接保证完整性。
    """
    clips = [AudioFileClip(str(p)) for p in clip_paths]
    try:
        # moviepy 2.2.x 的 concatenate_audioclips 不接受 method 参数；
        # 这里使用顺序拼接，确保多段 ACE-Step 输出不会只取第一段而导致歌曲不完整。
        merged = concatenate_audioclips(clips)

        merged.write_audiofile(
            str(output_path),
            fps=44100,
            bitrate=OUTPUT_BITRATE,
            codec="libmp3lame",
            logger=None,
        )
        duration = merged.duration
    finally:
        for c in clips:
            c.close()

    return {"duration": duration, "output_path": str(output_path)}


def generate_waveform_data(audio_path: Path, num_bars: int = 200) -> list[float]:
    """
    从音频文件提取波形数据（归一化到 0~1），用于前端波形可视化。
    返回 num_bars 个采样点的振幅列表。
    """
    with AudioFileClip(str(audio_path)) as clip:
        # 采样：每 (duration/num_bars) 秒取一个 RMS 值
        interval = clip.duration / num_bars
        bars = []
        for i in range(num_bars):
            t_start = i * interval
            t_end = min(t_start + interval, clip.duration)
            segment = clip.subclipped(t_start, t_end)
            # moviepy 2.2.1: get_frame 返回 numpy array
            frames = segment.get_frame(0)
            if frames.ndim > 1:
                frames = frames.mean(axis=1)
            rms = float(np.sqrt(np.mean(frames ** 2))) if len(frames) > 0 else 0.0
            bars.append(rms)

    # 归一化
    max_val = max(bars) if max(bars) > 0 else 1.0
    return [round(v / max_val, 4) for v in bars]


# ─────────────────────────────────────────────
# ACE-Step 任务轮询
# ─────────────────────────────────────────────
def poll_ace_step_task(task_id: str, timeout: int = 300, interval: int = 3) -> dict:
    """
    轮询 ACE-Step /query_result 接口，直到任务完成或超时。

    ACE-Step 1.5 官方状态码（注意：与自定义状态码不同）：
      0 = 排队中
      1 = 成功
      2 = 失败
      3 = 处理中
    """
    url = f"{ACE_STEP_BASE_URL}/query_result"
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            resp = requests.get(url, params={"task_id": task_id}, timeout=10)
            resp.raise_for_status()
            data = resp.json()

            status = data.get("status")
            logger.debug(f"ACE-Step 任务 {task_id} 状态: {status}")

            if status == 1:
                # 成功：返回音频文件路径列表
                return {"success": True, "audio_paths": data.get("audio_paths", [])}
            elif status == 2:
                # 失败
                return {"success": False, "error": data.get("error", "ACE-Step 生成失败")}
            # status 0/3: 继续等待

        except requests.RequestException as e:
            logger.warning(f"轮询 ACE-Step 失败: {e}，将在 {interval}s 后重试")

        time.sleep(interval)

    return {"success": False, "error": f"任务 {task_id} 超时（{timeout}s）"}


def download_ace_step_audio(audio_path: str, dest: Path) -> Path:
    """从 ACE-Step 服务下载生成的音频文件。"""
    url = f"{ACE_STEP_BASE_URL}/v1/audio"
    resp = requests.get(url, params={"path": audio_path}, timeout=60, stream=True)
    resp.raise_for_status()
    with open(dest, "wb") as f:
        for chunk in resp.iter_content(chunk_size=8192):
            f.write(chunk)
    logger.info(f"已下载原始音频: {dest} ({dest.stat().st_size / 1024:.1f} KB)")
    return dest


# ─────────────────────────────────────────────
# Java 回调
# ─────────────────────────────────────────────
def _callback_path(url: str) -> str:
    parsed = urlparse(url)
    return parsed.path or "/"


def build_internal_callback_headers(method: str, path: str, query: str = "") -> dict:
    """构造 Java 内部回调 HMAC 签名请求头，与 qijian-api 拦截器保持一致。"""
    headers = {"Content-Type": "application/json"}
    if not JAVA_CALLBACK_SECRET:
        logger.warning("JAVA_CALLBACK_SECRET 未设置，内部回调将不携带签名头，生产环境应拒绝此配置")
        return headers
    timestamp = str(int(time.time()))
    nonce = hashlib.sha256(f"{timestamp}:{os.getpid()}:{time.time_ns()}".encode("utf-8")).hexdigest()[:32]
    canonical = "\n".join([timestamp, nonce, method.upper(), path, query or ""])
    signature = hmac.new(
        JAVA_CALLBACK_SECRET.encode("utf-8"),
        canonical.encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()
    headers.update({
        "X-Internal-Timestamp": timestamp,
        "X-Internal-Nonce": nonce,
        "X-Internal-Signature": signature,
    })
    return headers


def notify_java_backend(task_id: str, payload: dict) -> bool:
    """通知 Java 后端任务处理结果。"""
    try:
        request_body = json.dumps({"taskId": task_id, **payload}, ensure_ascii=False, separators=(",", ":"))
        resp = requests.post(
            JAVA_CALLBACK_URL,
            data=request_body.encode("utf-8"),
            timeout=10,
            headers=build_internal_callback_headers("POST", _callback_path(JAVA_CALLBACK_URL), ""),
        )
        resp.raise_for_status()
        logger.info(f"Java 回调成功: taskId={task_id}")
        return True
    except Exception as e:
        logger.error(f"Java 回调失败: {e}")
        return False


# ─────────────────────────────────────────────
# 主处理流程
# ─────────────────────────────────────────────
def handle_task(task_data: dict, rdb: redis.Redis) -> None:
    """
    音乐生成后处理流程（新架构：qijian-infer 已完成推理，本程序只负责后处理）：
      1. 从 ACE-Step 服务下载原始音频（audioPath 由 qijian-infer 写入队列）
      2. moviepy 后处理（淡入淡出、截断、音量）
      3. 上传到 OSS
      4. 提取波形数据
      5. 回调 Java 后端
      6. 清理临时文件

    队列消息字段（由 qijian-infer 写入 melodai:audio_queue）：
      taskId    - 业务任务ID
      userId    - 用户ID
      aceTaskId - ACE-Step 任务ID（仅用于日志追踪）
      audioPath - ACE-Step 服务器上的原始音频路径（推理已完成）
      fadeIn    - 淡入时长（秒，默认 1.5）
      fadeOut   - 淡出时长（秒，默认 3.0）
      duration  - 目标时长（秒，0=不截断）
    """
    task_id = task_data.get("taskId", "unknown")
    ace_task_id = task_data.get("aceTaskId", "unknown")
    audio_path_str = task_data.get("audioPath", "")
    audio_paths_str = task_data.get("audioPaths", "")
    user_id = task_data.get("userId", "unknown")

    logger.info(f"开始后处理任务: taskId={task_id}, aceTaskId={ace_task_id}, audioPath={audio_path_str}")

    raw_audio_path = TEMP_DIR / f"{task_id}_raw.wav"
    merged_raw_path = TEMP_DIR / f"{task_id}_merged.wav"
    processed_path = TEMP_DIR / f"{task_id}_final.mp3"
    downloaded_paths: list[Path] = []

    try:
        # Step 1: 下载原始音频（推理已由 qijian-infer 完成，直接下载结果）
        audio_paths: list[str] = []
        if audio_paths_str:
            try:
                parsed_audio_paths = json.loads(audio_paths_str)
                if isinstance(parsed_audio_paths, list):
                    audio_paths = [str(p) for p in parsed_audio_paths if p]
            except json.JSONDecodeError:
                logger.warning("audioPaths 不是合法 JSON，将回退到 audioPath: %s", audio_paths_str)
        if not audio_paths and audio_path_str:
            audio_paths = [audio_path_str]
        if not audio_paths:
            raise RuntimeError("audioPath/audioPaths 为空，无法下载音频")

        for index, path_str in enumerate(audio_paths):
            part_path = raw_audio_path if len(audio_paths) == 1 else TEMP_DIR / f"{task_id}_raw_{index}.wav"
            download_ace_step_audio(path_str, part_path)
            downloaded_paths.append(part_path)

        input_audio_path = downloaded_paths[0]
        if len(downloaded_paths) > 1:
            merge_audio_clips(downloaded_paths, merged_raw_path)
            input_audio_path = merged_raw_path
            logger.info("已合并 %d 段原始音频用于完整歌曲后处理: %s", len(downloaded_paths), merged_raw_path)

        # Step 2: 音频后处理
        target_duration = float(task_data.get("duration", 0))
        process_result = process_audio(
            input_path=input_audio_path,
            output_path=processed_path,
            fade_in=float(task_data.get("fadeIn", DEFAULT_FADE_IN_SEC)),
            fade_out=float(task_data.get("fadeOut", DEFAULT_FADE_OUT_SEC)),
            max_duration=target_duration if target_duration > 0 else DEFAULT_MAX_DURATION,
            volume=float(task_data.get("volume", 1.0)),
        )

        # Step 3: 上传 OSS
        object_key = f"music/{user_id}/{task_id}.mp3"
        cdn_url = upload_to_oss(processed_path, object_key)

        # Step 4: 波形数据
        waveform = generate_waveform_data(processed_path, num_bars=200)

        # Step 5: 回调 Java
        notify_java_backend(task_id, {
            "status": 1,                              # 1=成功（Java侧自定义状态码）
            "audioUrl": cdn_url,
            "duration": int(process_result["duration"]),
            "waveform": waveform,
        })
        # 更新 Redis（使用 String 类型，与 Java 側 StringRedisTemplate.opsForValue 保持一致）
        # 注意：不能使用 hset，Java 层用 opsForValue.set 写入的是 String类型，
        # 混用不同类型会抛出 Redis WRONGTYPE 错误
        rdb.set(f"melodai:task:{task_id}", "1", ex=86400 * 7)   # 1=成功, 7天 TTL
        logger.info(f"任务完成: taskId={task_id}, url={cdn_url}")
    except Exception as e:
        error_msg = str(e)
        logger.error(f"任务失败: taskId={task_id}, error={error_msg}\n{traceback.format_exc()}")
        notify_java_backend(task_id, {
            "status": 2,                              # 2=失败（Java侧自定义状态码）
            "error": error_msg,
        })
        # 失败时同样使用 String 类型，与 Java 层保持一致
        rdb.set(f"melodai:task:{task_id}", "2", ex=3600)         # 2=失败, 保畡1小时留1小时

    finally:
        # Step 6: 清理临时文件
        cleanup_paths = list(dict.fromkeys([raw_audio_path, merged_raw_path, processed_path, *downloaded_paths]))
        for path in cleanup_paths:
            if path.exists():
                path.unlink()
                logger.debug(f"已清理临时文件: {path}")


# ─────────────────────────────────────────────
# Worker 主循环（Redis Stream）
# ─────────────────────────────────────────────
def _process_pending_messages(rdb: redis.Redis) -> None:
    """启动时认领并重试长时间未确认的 Pending 消息。"""
    try:
        pending = rdb.xpending_range(
            QUEUE_KEY,
            CONSUMER_GROUP,
            min="-",
            max="+",
            count=PENDING_RETRY_COUNT,
        )
        if not pending:
            return
        logger.info("发现 %d 条音频后处理 Pending 消息，开始恢复处理", len(pending))
        for item in pending:
            msg_id = item.get("message_id") if isinstance(item, dict) else item[0]
            idle = item.get("time_since_delivered") if isinstance(item, dict) else item[2]
            if idle is not None and int(idle) < PENDING_IDLE_MS:
                continue
            claimed_entries = rdb.xclaim(
                QUEUE_KEY,
                CONSUMER_GROUP,
                CONSUMER_NAME,
                min_idle_time=PENDING_IDLE_MS,
                message_ids=[msg_id],
            )
            for claimed_msg_id, fields in claimed_entries:
                try:
                    logger.info("重新处理音频后处理 Pending 消息: msg_id=%s", claimed_msg_id)
                    handle_task(fields, rdb)
                    rdb.xack(QUEUE_KEY, CONSUMER_GROUP, claimed_msg_id)
                except Exception as exc:
                    logger.error("Pending 音频消息处理失败: msg_id=%s error=%s", claimed_msg_id, exc)
    except redis.exceptions.ResponseError as exc:
        logger.warning("读取音频后处理 Pending 消息失败，跳过本轮恢复: %s", exc)


def run_worker() -> None:
    """
    使用 Redis Stream（XREADGROUP）消费任务队列。
    相比 Redis List，Stream 支持消费确认（XACK），避免 Worker 崩溃时消息丢失。
    """
    rdb = get_redis_client()

    # 创建 Stream 和消费者组（幂等）
    try:
        rdb.xgroup_create(QUEUE_KEY, CONSUMER_GROUP, id="0", mkstream=True)
        logger.info(f"消费者组 '{CONSUMER_GROUP}' 已创建")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            logger.info(f"消费者组 '{CONSUMER_GROUP}' 已存在，跳过创建")
        else:
            raise

    logger.info(f"Worker 启动: {CONSUMER_NAME}，监听队列: {QUEUE_KEY}")
    _process_pending_messages(rdb)

    while True:
        try:
            # 读取新消息（阻塞等待 5 秒）
            messages = rdb.xreadgroup(
                groupname=CONSUMER_GROUP,
                consumername=CONSUMER_NAME,
                streams={QUEUE_KEY: ">"},
                count=1,
                block=5000,
            )

            if not messages:
                continue

            for stream_key, entries in messages:
                for msg_id, fields in entries:
                    try:
                        # qijian-infer 直接使用 xadd(key, dict) 写入字段，无需 json.loads
                        task_data = fields
                        handle_task(task_data, rdb)
                        # 确认消息已处理
                        rdb.xack(QUEUE_KEY, CONSUMER_GROUP, msg_id)
                    except Exception as e:
                        logger.error(f"消息处理异常: msg_id={msg_id}, error={e}")
                        # 不 XACK，消息将进入 PEL，可通过 XPENDING 监控和重试

        except redis.exceptions.ConnectionError as e:
            logger.error(f"Redis 连接断开: {e}，5秒后重连...")
            time.sleep(5)
            rdb = get_redis_client()
        except KeyboardInterrupt:
            logger.info("Worker 收到停止信号，正在退出...")
            break


# ─────────────────────────────────────────────
# 入口
# ─────────────────────────────────────────────
if __name__ == "__main__":
    run_worker()
