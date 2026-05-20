# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概述

**qijian-worker** 是七剑音乐生成系统的 Python 音频后处理服务。它从 Redis Stream 队列消费任务，在 AI 推理阶段（由 `qijian-infer` 完成）之后，使用 moviepy 2.2.1 进行音频后处理。

### 系统架构

本项目是分布式系统的一部分，包含三个主要组件：
1. **qijian-api** (Java/Spring Boot，端口 80) - REST API 和业务逻辑
2. **qijian-infer** (Python) - ACE-Step 1.5 推理封装，写入任务到 Redis Stream
3. **qijian-worker** (本仓库) - 音频后处理、OSS 上传、Java 回调

**数据流向：**
```
qijian-api → qijian-infer → ACE-Step 1.5 推理 → Redis Stream → qijian-worker → OSS + Java 回调
```

### 核心技术栈

- **moviepy 2.2.1**：音频处理库，相比 1.x 版本有破坏性 API 变更
  - 使用 `.with_effects([...])` 替代方法链式调用
  - `AudioFadeIn`、`AudioFadeOut`、`MultiplyVolume` 作为 fx 对象使用
  - `write_audiofile` 无 `verbose` 参数，使用 `logger=None`
- **Redis Stream**：任务队列，支持消费者组（XREADGROUP/XACK）
- **boto3**：OSS/S3 上传（兼容阿里云 OSS 和 AWS S3）

## 开发命令

### 本地开发
```bash
# 安装依赖
pip install -r requirements.txt

# 本地运行 worker
python3 audio_processor.py

# 注意：需要在环境变量中配置 Redis、ACE-Step 服务和 OSS 凭证
```

### Docker
```bash
# 构建镜像
docker build -t qijian-worker .

# 运行容器
docker run --rm \
  -e REDIS_HOST=redis-host \
  -e REDIS_PASSWORD=your-password \
  -e ACE_STEP_BASE_URL=http://ace-step:8000 \
  -e OSS_ENDPOINT=https://oss-cn-hangzhou.aliyuncs.com \
  -e OSS_ACCESS_KEY=your-key \
  -e OSS_SECRET_KEY=your-secret \
  -e JAVA_CALLBACK_URL=http://qijian-api:80/api/music/callback \
  qijian-worker
```

## 核心架构

### Redis 集成

**队列结构：**
- **Stream Key**：`melodai:audio_queue`
- **消费者组**：`qijian-worker-group`
- **消息字段**（由 qijian-infer 写入）：
  - `taskId`：业务任务 ID
  - `userId`：用户 ID，用于 OSS 路径
  - `aceTaskId`：ACE-Step 任务 ID（用于日志追踪）
  - `audioPath`：ACE-Step 服务器上原始音频的路径
  - `fadeIn`：淡入时长（秒，默认 1.5）
  - `fadeOut`：淡出时长（秒，默认 3.0）
  - `duration`：目标时长（秒，0 表示不限制）
  - `volume`：音量倍数（默认 1.0）

**任务状态存储：**
- **Key 模式**：`melodai:task:{taskId}`
- **类型**：Redis String（**不是** Hash - Java 使用 `StringRedisTemplate.opsForValue`）
- **值**：`"1"`（成功）或 `"2"`（失败）
- **TTL**：成功 7 天，失败 1 小时

**重要警告**：绝对不要使用 `hset` 来存储任务状态 - Java 端使用 `opsForValue.set()` 创建的是 String 类型。混用不同类型会导致 `WRONGTYPE` 错误。

### 处理流程

函数：`handle_task(task_data, rdb)` 位于 audio_processor.py:330

1. **下载**：通过 `/v1/audio?path={audioPath}` 从 ACE-Step 服务获取原始音频
2. **处理**：使用 `process_audio()` 应用淡入淡出、音量、时长限制
3. **上传**：上传到 OSS，路径为 `music/{userId}/{taskId}.mp3`
4. **波形**：提取 200 个采样点的波形数据用于前端可视化
5. **回调**：POST 到 Java API `/api/music/callback`，包含状态/URL/时长/波形
6. **清理**：删除临时文件

### 音频处理细节

函数：`process_audio()` 位于 audio_processor.py:126

**moviepy 2.2.1 API 变更：**
```python
# 旧版 (1.x) - 不要使用
clip.fadein(1.5).fadeout(3.0).volumex(0.8)

# 新版 (2.2.1) - 正确写法
clip.with_effects([
    AudioFadeIn(1.5),
    AudioFadeOut(3.0),
    MultiplyVolume(0.8)
])
```

**导出设置：**
- 格式：MP3 (libmp3lame 编码器)
- 采样率：44100 Hz
- 码率：192k
- 使用 `logger=None` 抑制 moviepy 日志输出

### 环境变量

**Redis（必需）：**
- `REDIS_HOST`：Redis 主机名（默认：127.0.0.1）
- `REDIS_PORT`：Redis 端口（默认：6379）
- `REDIS_PASSWORD`：**生产环境必须设置**
- `REDIS_DB`：数据库编号（默认：0）

**服务：**
- `ACE_STEP_BASE_URL`：ACE-Step 推理服务地址（默认：http://127.0.0.1:8000）
- `JAVA_CALLBACK_URL`：Java API 回调端点（默认：http://127.0.0.1:80/api/music/callback）

**OSS/S3（上传必需）：**
- `OSS_ENDPOINT`：OSS 端点（如：https://oss-cn-hangzhou.aliyuncs.com）
- `OSS_BUCKET`：存储桶名称（默认：melodai-music）
- `OSS_ACCESS_KEY`：访问密钥 ID
- `OSS_SECRET_KEY`：密钥
- `OSS_CDN_PREFIX`：CDN URL 前缀（默认：https://cdn.melodai.com）

### 错误处理

**事务安全性：**
- 未被 XACK 的任务会保留在 Redis Stream Pending Entries List (PEL) 中
- 使用 `XPENDING` 命令监控卡住的消息
- 由于消费者组确认机制，Worker 崩溃不会丢失消息

**回调协议：**
```json
{
  "taskId": "uuid",
  "status": 1,           // 1=成功, 2=失败（Java 自定义状态码）
  "audioUrl": "https://cdn.melodai.com/music/userId/taskId.mp3",
  "duration": 180,       // 秒
  "waveform": [0.1, 0.2, ...],  // 200 个浮点数
  "error": "错误信息"      // 仅失败时返回
}
```

### ACE-Step 集成

**状态码**（ACE-Step 1.5 官方，与 Java 自定义状态码不同）：
- `0`：排队中
- `1`：成功
- `2`：失败
- `3`：处理中

**注意**：`poll_ace_step_task()` 是遗留代码 - qijian-infer 现在负责轮询。本 worker 仅通过 `download_ace_step_audio()` 下载预生成的音频。

## 重要约束

1. **无测试框架**：仓库中没有测试 - 需要手动验证
2. **单文件结构**：所有逻辑在 `audio_processor.py` 中 - 没有模块化结构
3. **临时文件**：使用 `/tmp/melodai_audio/` - 确保有足够的磁盘空间
4. **FFmpeg 依赖**：moviepy 需要 ffmpeg 二进制文件（已包含在 Docker 镜像中）
5. **Redis 类型安全**：任务状态必须使用 `rdb.set()`，绝不能用 `hset()`
6. **moviepy 2.2.1**：相比 1.x 有破坏性变更 - 重构前请查阅 API 文档

## 调试技巧

**检查 Redis 队列：**
```bash
# 查看待处理消息
redis-cli XPENDING melodai:audio_queue qijian-worker-group

# 查看消费者组信息
redis-cli XINFO GROUPS melodai:audio_queue

# 手动读取消息
redis-cli XREAD COUNT 1 STREAMS melodai:audio_queue 0
```

**本地测试音频处理：**
```python
from audio_processor import process_audio
from pathlib import Path

result = process_audio(
    input_path=Path("test.wav"),
    output_path=Path("output.mp3"),
    fade_in=1.5,
    fade_out=3.0,
)
print(result)
```

**Docker 日志：**
```bash
docker logs -f qijian-worker
```
