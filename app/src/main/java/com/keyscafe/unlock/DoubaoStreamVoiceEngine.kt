package com.keyscafe.unlock

import android.media.AudioFormat
import android.media.AudioRecord
import android.media.MediaRecorder
import android.os.Handler
import android.os.Looper
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import okhttp3.WebSocket
import okhttp3.WebSocketListener
import okio.ByteString
import okio.ByteString.Companion.toByteString
import org.json.JSONObject
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream
import kotlin.concurrent.thread
import kotlin.math.sqrt

/**
 * 豆包大模型流式语音识别引擎（Seed-ASR 2.0 WebSocket）
 */
object DoubaoStreamVoiceEngine {

    private const val WS_URL = "wss://openspeech.bytedance.com/api/v3/sauc/bigmodel_async"
    private const val SAMPLE_RATE = 16000
    private const val FRAME_MS = 100
    private const val FRAME_BYTES = SAMPLE_RATE * 2 * FRAME_MS / 1000

    private const val PROTOCOL_VERSION = 1
    private const val DEFAULT_HEADER_SIZE = 1

    private const val MSG_FULL_CLIENT_REQUEST = 1
    private const val MSG_AUDIO_ONLY_REQUEST = 2
    private const val MSG_FULL_SERVER_RESPONSE = 9
    private const val MSG_SERVER_ACK = 11
    private const val MSG_ERROR_SERVER_RESPONSE = 15

    private const val FLAG_NO_SEQUENCE = 0b0000
    private const val FLAG_POS_SEQUENCE = 0b0001
    private const val FLAG_NEG_SEQUENCE = 0b0010

    private const val SERIAL_NONE = 0
    private const val SERIAL_JSON = 1
    private const val COMPRESSION_GZIP = 1

    private val mainHandler = Handler(Looper.getMainLooper())
    private val isRecording = AtomicBoolean(false)
    private val sequenceCounter = AtomicInteger(1)

    @Volatile private var currentWebSocket: WebSocket? = null
    @Volatile private var currentAudioRecord: AudioRecord? = null
    @Volatile private var lastRecognizedText: String = ""

    private val okHttpClient = OkHttpClient.Builder()
        .connectTimeout(4, TimeUnit.SECONDS)
        .readTimeout(8, TimeUnit.SECONDS)
        .pingInterval(5, TimeUnit.SECONDS)
        .build()

    interface StreamCallback {
        fun onListeningStarted()
        fun onVolumeChanged(rmsDb: Float)
        fun onPartialText(partialText: String)
        fun onFinalText(finalText: String)
        fun onError(errorMsg: String)
    }

    /** 异步启动录音与流式推流，不阻塞主线程动画 */
    fun start(
        apiKey: String,
        appId: String = "",
        cluster: String = "volc.seedasr.sauc.duration",
        callback: StreamCallback
    ) {
        if (apiKey.isBlank()) {
            postError(callback, "请先在汐语设置中填写豆包 API Key")
            return
        }
        if (isRecording.getAndSet(true)) return

        lastRecognizedText = ""
        sequenceCounter.set(1)

        thread(name = "xy-doubao-worker") {
            val minBufSize = AudioRecord.getMinBufferSize(
                SAMPLE_RATE,
                AudioFormat.CHANNEL_IN_MONO,
                AudioFormat.ENCODING_PCM_16BIT
            )
            if (minBufSize <= 0) {
                isRecording.set(false)
                postError(callback, "设备不支持 16kHz 单声道音频采集")
                return@thread
            }

            val recorder = runCatching {
                AudioRecord(
                    MediaRecorder.AudioSource.VOICE_RECOGNITION,
                    SAMPLE_RATE,
                    AudioFormat.CHANNEL_IN_MONO,
                    AudioFormat.ENCODING_PCM_16BIT,
                    maxOf(minBufSize * 2, FRAME_BYTES * 8)
                )
            }.getOrNull()

            if (recorder == null || recorder.state != AudioRecord.STATE_INITIALIZED) {
                recorder?.release()
                isRecording.set(false)
                postError(callback, "麦克风初始化失败，请检查录音权限")
                return@thread
            }

            currentAudioRecord = recorder
            try {
                recorder.startRecording()
            } catch (e: Exception) {
                recorder.release()
                currentAudioRecord = null
                isRecording.set(false)
                postError(callback, "启动录音失败: ${e.message}")
                return@thread
            }

            mainHandler.post { callback.onListeningStarted() }

            val preConnectQueue = ConcurrentLinkedQueue<ByteArray>()
            val activeSocket = AtomicReference<WebSocket?>(null)
            val isHandshakeDone = AtomicBoolean(false)

            val reqId = UUID.randomUUID().toString()
            val request = buildWebSocketRequest(apiKey, appId, cluster, reqId)

            currentWebSocket = okHttpClient.newWebSocket(request, object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: Response) {
                    activeSocket.set(webSocket)
                    val initPayload = createInitPayloadJson()
                    val initPacket = packBinaryPacket(
                        messageType = MSG_FULL_CLIENT_REQUEST,
                        flags = FLAG_POS_SEQUENCE,
                        sequence = sequenceCounter.getAndIncrement(),
                        payload = gzipCompress(initPayload.toByteArray(Charsets.UTF_8))
                    )
                    webSocket.send(initPacket.toByteString())

                    while (true) {
                        val chunk = preConnectQueue.poll() ?: break
                        val seq = sequenceCounter.getAndIncrement()
                        val packet = packBinaryPacket(
                            messageType = MSG_AUDIO_ONLY_REQUEST,
                            flags = FLAG_POS_SEQUENCE,
                            sequence = seq,
                            payload = gzipCompress(chunk)
                        )
                        webSocket.send(packet.toByteString())
                    }
                    isHandshakeDone.set(true)
                }

                override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
                    if (currentWebSocket !== webSocket) return
                    parseServerResponse(bytes.toByteArray(), callback)
                }

                override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                    XLog.e("DoubaoStreamVoice: 连接异常: ${t.message}", t)
                    val code = response?.code
                    val err = when (code) {
                        401, 403 -> "鉴权失败 ($code)，请检查 API Key"
                        404 -> "资源 ID 不存在 ($code)，请检查 Cluster"
                        else -> "网络连接异常: ${t.message ?: "超时"}"
                    }
                    postError(callback, err)
                    stop()
                }
            })

            val buffer = ByteArray(FRAME_BYTES)
            while (isRecording.get()) {
                val read = recorder.read(buffer, 0, FRAME_BYTES)
                if (read <= 0) break
                val chunk = if (read == FRAME_BYTES) buffer.clone() else buffer.copyOf(read)
                val rms = calculateRms(chunk)
                mainHandler.post { callback.onVolumeChanged(rms.toFloat()) }

                val ws = activeSocket.get()
                if (ws != null && isHandshakeDone.get()) {
                    val seq = sequenceCounter.getAndIncrement()
                    val packet = packBinaryPacket(
                        messageType = MSG_AUDIO_ONLY_REQUEST,
                        flags = FLAG_POS_SEQUENCE,
                        sequence = seq,
                        payload = gzipCompress(chunk)
                    )
                    ws.send(packet.toByteString())
                } else if (preConnectQueue.size < 20) {
                    preConnectQueue.offer(chunk)
                }
            }
        }
    }

    /** 停止录音并立即交付当前文字，异步发送尾包并安全回收 */
    fun stop(onCompleted: ((String) -> Unit)? = null) {
        if (!isRecording.getAndSet(false)) return

        val recorder = currentAudioRecord
        currentAudioRecord = null
        runCatching {
            recorder?.stop()
            recorder?.release()
        }

        val immediateText = lastRecognizedText
        onCompleted?.invoke(immediateText)

        val ws = currentWebSocket
        if (ws != null) {
            val seq = -sequenceCounter.getAndIncrement()
            val endPacket = packBinaryPacket(
                messageType = MSG_AUDIO_ONLY_REQUEST,
                flags = FLAG_NEG_SEQUENCE,
                sequence = seq,
                payload = gzipCompress(ByteArray(0))
            )
            runCatching { ws.send(endPacket.toByteString()) }

            mainHandler.postDelayed({
                runCatching { ws.close(1000, "Done") }
                if (currentWebSocket === ws) currentWebSocket = null
            }, 300)
        }
    }

    /** 取消当前录音与识别 */
    fun cancel() {
        isRecording.set(false)
        val recorder = currentAudioRecord
        currentAudioRecord = null
        runCatching {
            recorder?.stop()
            recorder?.release()
        }
        val ws = currentWebSocket
        currentWebSocket = null
        runCatching { ws?.cancel() }
        lastRecognizedText = ""
    }

    /** 测试接口连通性与凭证有效性 */
    fun testConnection(
        apiKey: String,
        appId: String = "",
        cluster: String = "volc.seedasr.sauc.duration",
        onResult: (Boolean, String) -> Unit
    ) {
        if (apiKey.isBlank()) {
            onResult(false, "API Key 不能为空")
            return
        }

        val reqId = UUID.randomUUID().toString()
        val request = buildWebSocketRequest(apiKey, appId, cluster, reqId)

        thread(name = "xy-doubao-test") {
            var finished = false
            val ws = okHttpClient.newWebSocket(request, object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: Response) {
                    val initPayload = createInitPayloadJson()
                    val initPacket = packBinaryPacket(
                        messageType = MSG_FULL_CLIENT_REQUEST,
                        flags = FLAG_POS_SEQUENCE,
                        sequence = 1,
                        payload = gzipCompress(initPayload.toByteArray(Charsets.UTF_8))
                    )
                    webSocket.send(initPacket.toByteString())
                }

                override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
                    if (finished) return
                    finished = true
                    val data = bytes.toByteArray()
                    if (data.size >= 2) {
                        val msgType = (data[1].toInt() shr 4) and 0x0F
                        if (msgType == MSG_ERROR_SERVER_RESPONSE) {
                            val (errCode, errMsg) = parseErrorResponse(data)
                            mainHandler.post { onResult(false, "服务端报错 ($errCode): $errMsg") }
                            webSocket.close(1000, "Done")
                            return
                        }
                        if (msgType == MSG_SERVER_ACK || msgType == MSG_FULL_SERVER_RESPONSE) {
                            mainHandler.post { onResult(true, "连接成功！豆包流式服务已就绪") }
                            webSocket.close(1000, "Done")
                            return
                        }
                    }

                    val resp = extractResponseJson(data)
                    val code = resp?.first ?: -1
                    if (code == 1000 || code == 20000000) {
                        mainHandler.post { onResult(true, "连接成功！豆包流式服务已就绪") }
                    } else {
                        val (errCode, errMsg) = parseErrorResponse(data)
                        mainHandler.post { onResult(false, "握手响应失败 ($errCode): $errMsg") }
                    }
                    webSocket.close(1000, "Done")
                }

                override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                    if (finished) return
                    finished = true
                    val code = response?.code
                    val msg = when (code) {
                        401, 403 -> "鉴权失败 ($code)，请检查 API Key"
                        404 -> "资源 ID 不存在 ($code)，请检查 Cluster"
                        else -> "网络连接失败: ${t.message ?: "超时"}"
                    }
                    mainHandler.post { onResult(false, msg) }
                }
            })

            mainHandler.postDelayed({
                if (!finished) {
                    finished = true
                    runCatching { ws.cancel() }
                    onResult(false, "连接超时，请检查网络环境")
                }
            }, 6000)
        }
    }

    private fun buildWebSocketRequest(
        apiKey: String,
        appId: String,
        cluster: String,
        reqId: String
    ): Request {
        val targetCluster = if (cluster.isBlank()) "volc.seedasr.sauc.duration" else cluster.trim()
        val builder = Request.Builder()
            .url(WS_URL)
            .addHeader("X-Api-Resource-Id", targetCluster)
            .addHeader("X-Api-Request-Id", reqId)
            .addHeader("X-Api-Connect-Id", reqId)

        if (appId.isNotBlank()) {
            builder.addHeader("X-Api-App-Key", appId.trim())
            builder.addHeader("X-Api-Access-Key", apiKey.trim())
        } else {
            builder.addHeader("X-Api-Key", apiKey.trim())
        }
        return builder.build()
    }

    private fun parseServerResponse(data: ByteArray, callback: StreamCallback) {
        if (data.size >= 2) {
            val msgType = (data[1].toInt() shr 4) and 0x0F
            if (msgType == MSG_SERVER_ACK) return
            if (msgType == MSG_ERROR_SERVER_RESPONSE) {
                val (errCode, errMsg) = parseErrorResponse(data)
                XLog.e("DoubaoStreamVoice: 服务端报错 code=$errCode msg=$errMsg", null)
                postError(callback, "识别服务报错 ($errCode): $errMsg")
                return
            }
        }

        val (code, jsonStr) = extractResponseJson(data) ?: return
        if (code != 1000 && code != 20000000) {
            XLog.e("DoubaoStreamVoice: 错误码 code=$code raw=$jsonStr", null)
            postError(callback, "识别服务提示错误 ($code)")
            return
        }

        runCatching {
            val root = JSONObject(jsonStr)
            val text = root.optJSONObject("result")?.optString("text").orEmpty()
            if (text.isNotBlank()) {
                lastRecognizedText = text
                mainHandler.post { callback.onPartialText(text) }
            }
        }.onFailure { e ->
            XLog.e("DoubaoStreamVoice: 解析响应 JSON 失败", e)
        }
    }

    private fun parseErrorResponse(data: ByteArray): Pair<Int, String> {
        if (data.size < 8) return Pair(-1, "未知服务端错误")
        val code = runCatching {
            ByteBuffer.wrap(data, 4, 4).order(ByteOrder.BIG_ENDIAN).int
        }.getOrDefault(-1)

        val rawMsg = if (data.size >= 12) {
            val size = runCatching {
                ByteBuffer.wrap(data, 8, 4).order(ByteOrder.BIG_ENDIAN).int
            }.getOrDefault(0)
            if (size in 1..2048 && data.size >= 12 + size) {
                String(data, 12, size, Charsets.UTF_8)
            } else if (data.size > 12) {
                String(data, 12, data.size - 12, Charsets.UTF_8)
            } else {
                ""
            }
        } else {
            ""
        }

        val cleanMsg = runCatching {
            JSONObject(rawMsg).optString("error", rawMsg)
        }.getOrDefault(rawMsg)

        return Pair(code, cleanMsg.ifBlank { "错误码 $code" })
    }

    private fun extractResponseJson(data: ByteArray): Pair<Int, String>? {
        if (data.size < 8) return null
        val compMethod = data[2].toInt() and 0x0F

        // 兼容 V1 协议含 4 字节 Sequence（偏移 8）与无 Sequence（偏移 4）两种分包
        val offsets = intArrayOf(8, 4)
        for (sizeOffset in offsets) {
            if (data.size < sizeOffset + 4) continue
            val payloadSize = ByteBuffer.wrap(data, sizeOffset, 4).order(ByteOrder.BIG_ENDIAN).int
            val payloadStart = sizeOffset + 4
            if (payloadSize <= 0 || data.size < payloadStart + payloadSize) continue

            val rawPayload = data.copyOfRange(payloadStart, payloadStart + payloadSize)
            val decompressed = runCatching {
                if (compMethod == COMPRESSION_GZIP) gzipDecompress(rawPayload) else rawPayload
            }.getOrNull() ?: continue

            val jsonStr = runCatching { String(decompressed, Charsets.UTF_8) }.getOrNull() ?: continue
            val code = runCatching { JSONObject(jsonStr).optInt("code", -1) }.getOrDefault(-1)
            if (code != -1) {
                return Pair(code, jsonStr)
            }
        }
        return null
    }

    private fun createInitPayloadJson(): String {
        return JSONObject().apply {
            put("user", JSONObject().apply {
                put("uid", "xiyu_ime_" + UUID.randomUUID().toString().take(8))
            })
            put("audio", JSONObject().apply {
                put("format", "pcm")
                put("codec", "raw")
                put("rate", SAMPLE_RATE)
                put("bits", 16)
                put("channel", 1)
            })
            put("request", JSONObject().apply {
                put("model_name", "bigmodel")
                put("enable_itn", true)
                put("enable_punc", true)
                put("result_type", "full")
            })
        }.toString()
    }

    private fun packBinaryPacket(
        messageType: Int,
        flags: Int,
        sequence: Int,
        payload: ByteArray
    ): ByteArray {
        val header = ByteArray(4)
        header[0] = ((PROTOCOL_VERSION shl 4) or DEFAULT_HEADER_SIZE).toByte()
        header[1] = ((messageType shl 4) or (flags and 0x0F)).toByte()
        val serialization = if (messageType == MSG_FULL_CLIENT_REQUEST) SERIAL_JSON else SERIAL_NONE
        header[2] = ((serialization shl 4) or COMPRESSION_GZIP).toByte()
        header[3] = 0x00

        val hasSequence = (flags and 0x03) != 0
        val totalSize = 4 + (if (hasSequence) 4 else 0) + 4 + payload.size
        return ByteBuffer.allocate(totalSize).order(ByteOrder.BIG_ENDIAN).apply {
            put(header)
            if (hasSequence) {
                putInt(sequence)
            }
            putInt(payload.size)
            put(payload)
        }.array()
    }

    private fun gzipCompress(data: ByteArray): ByteArray {
        val bos = ByteArrayOutputStream()
        GZIPOutputStream(bos).use { it.write(data) }
        return bos.toByteArray()
    }

    private fun gzipDecompress(data: ByteArray): ByteArray {
        val bis = ByteArrayInputStream(data)
        val bos = ByteArrayOutputStream()
        GZIPInputStream(bis).use { it.copyTo(bos) }
        return bos.toByteArray()
    }

    private fun calculateRms(pcm: ByteArray): Double {
        var sumSq = 0.0
        var i = 0
        while (i < pcm.size - 1) {
            val sample = (pcm[i].toInt() and 0xFF) or (pcm[i + 1].toInt() shl 8)
            val s = sample.toShort().toDouble()
            sumSq += s * s
            i += 2
        }
        val linearRms = sqrt(sumSq / (pcm.size / 2))
        return if (linearRms > 1.0) 20 * kotlin.math.log10(linearRms) else 0.0
    }

    private fun postError(callback: StreamCallback, msg: String) {
        mainHandler.post { callback.onError(msg) }
    }
}
