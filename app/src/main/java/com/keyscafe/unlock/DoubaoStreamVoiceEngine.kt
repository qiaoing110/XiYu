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
    // 50ms 分帧（1600 字节）
    private const val FRAME_MS = 50
    private const val FRAME_BYTES = SAMPLE_RATE * 2 * FRAME_MS / 1000
    private const val MAX_PRE_CONNECT_FRAMES = 60 // 握手前最多暂存 3s 音频
    private const val TAIL_WAIT_TIMEOUT_MS = 280L // 松手等待尾包的最大收敛时间

    private const val PROTOCOL_VERSION = 1
    private const val DEFAULT_HEADER_SIZE = 1

    private const val MSG_FULL_CLIENT_REQUEST = 1
    private const val MSG_AUDIO_ONLY_REQUEST = 2
    private const val MSG_FULL_SERVER_RESPONSE = 9
    private const val MSG_SERVER_ACK = 11
    private const val MSG_ERROR_SERVER_RESPONSE = 15

    private const val FLAG_NO_SEQUENCE = 0b0000
    private const val FLAG_POS_SEQUENCE = 0b0001
    // 负序号尾包（含 sequence 且 sequence 为负）
    private const val FLAG_NEG_SEQUENCE = 0b0011

    private const val SERIAL_NONE = 0
    private const val SERIAL_JSON = 1
    private const val COMPRESSION_GZIP = 1

    private val mainHandler = Handler(Looper.getMainLooper())
    private val isRecording = AtomicBoolean(false)
    private val sequenceCounter = AtomicInteger(1)
    private val sessionGenerator = AtomicInteger(0)

    @Volatile
    private var activeSessionId: Int = 0

    @Volatile
    private var currentWebSocket: WebSocket? = null

    @Volatile
    private var lastRecognizedText: String = ""

    @Volatile
    private var pendingStopCallback: ((String) -> Unit)? = null

    private val stopDelivered = AtomicBoolean(false)

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
        // 若上一轮仍在等待尾包，立即触发其完成回调并清理旧连接
        flushPendingStop()
        if (isRecording.getAndSet(true)) return

        val mySessionId = sessionGenerator.incrementAndGet()
        activeSessionId = mySessionId
        lastRecognizedText = ""
        sequenceCounter.set(1)
        stopDelivered.set(false)
        pendingStopCallback = null

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
                    MediaRecorder.AudioSource.MIC,
                    SAMPLE_RATE,
                    AudioFormat.CHANNEL_IN_MONO,
                    AudioFormat.ENCODING_PCM_16BIT,
                    maxOf(minBufSize * 2, FRAME_BYTES * 4)
                )
            }.getOrNull()

            if (recorder == null || recorder.state != AudioRecord.STATE_INITIALIZED) {
                runCatching { recorder?.release() }
                isRecording.set(false)
                postError(callback, "麦克风初始化失败，请检查录音权限")
                return@thread
            }

            try {
                recorder.startRecording()
            } catch (e: Throwable) {
                runCatching { recorder.release() }
                isRecording.set(false)
                postError(callback, "无法启动麦克风录音: ${e.message}")
                return@thread
            }

            mainHandler.post {
                if (activeSessionId == mySessionId) callback.onListeningStarted()
            }

            val preConnectQueue = ConcurrentLinkedQueue<ByteArray>()
            val activeSocket = AtomicReference<WebSocket?>(null)
            val isHandshakeDone = AtomicBoolean(false)

            fun drainPreConnectQueue(ws: WebSocket) {
                while (true) {
                    val chunk = preConnectQueue.poll() ?: break
                    val seq = sequenceCounter.getAndIncrement()
                    val packet = packBinaryPacket(
                        messageType = MSG_AUDIO_ONLY_REQUEST,
                        flags = FLAG_POS_SEQUENCE,
                        sequence = seq,
                        payload = gzipCompress(chunk)
                    )
                    ws.send(packet.toByteString())
                }
            }

            val reqId = UUID.randomUUID().toString()
            val request = buildWebSocketRequest(apiKey, appId, cluster, reqId)

            val wsInstance = okHttpClient.newWebSocket(request, object : WebSocketListener() {
                override fun onOpen(webSocket: WebSocket, response: Response) {
                    if (activeSessionId != mySessionId) {
                        runCatching { webSocket.close(1000, "Stale") }
                        return
                    }
                    activeSocket.set(webSocket)
                    val initPayload = createInitPayloadJson()
                    val initPacket = packBinaryPacket(
                        messageType = MSG_FULL_CLIENT_REQUEST,
                        flags = FLAG_POS_SEQUENCE,
                        sequence = sequenceCounter.getAndIncrement(),
                        payload = gzipCompress(initPayload.toByteArray(Charsets.UTF_8))
                    )
                    webSocket.send(initPacket.toByteString())
                    drainPreConnectQueue(webSocket)
                    isHandshakeDone.set(true)
                    drainPreConnectQueue(webSocket) // 二次排空，消除 isHandshakeDone 置位瞬间的竞态丢帧
                }

                override fun onMessage(webSocket: WebSocket, bytes: ByteString) {
                    if (activeSessionId != mySessionId) return
                    parseServerResponse(mySessionId, bytes.toByteArray(), callback)
                }

                override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                    if (activeSessionId != mySessionId) return
                    XLog.e("DoubaoStreamVoice: WebSocket 异常", t)
                    isRecording.set(false)
                    if (!deliverStopIfPending(mySessionId, lastRecognizedText)) {
                        postError(callback, "语音网络连接中断: ${t.message ?: "未知错误"}")
                    }
                }
            })
            currentWebSocket = wsInstance

            val buffer = ByteArray(FRAME_BYTES)
            try {
                while (isRecording.get() && activeSessionId == mySessionId) {
                    val read = recorder.read(buffer, 0, FRAME_BYTES)
                    if (read > 0) {
                        val chunk = if (read == FRAME_BYTES) buffer.clone() else buffer.copyOf(read)
                        val rms = calculateRms(chunk)
                        mainHandler.post {
                            if (activeSessionId == mySessionId && isRecording.get()) {
                                callback.onVolumeChanged(rms.toFloat())
                            }
                        }

                        val ws = activeSocket.get()
                        if (ws != null && isHandshakeDone.get()) {
                            drainPreConnectQueue(ws)
                            val seq = sequenceCounter.getAndIncrement()
                            val packet = packBinaryPacket(
                                messageType = MSG_AUDIO_ONLY_REQUEST,
                                flags = FLAG_POS_SEQUENCE,
                                sequence = seq,
                                payload = gzipCompress(chunk)
                            )
                            ws.send(packet.toByteString())
                        } else {
                            if (preConnectQueue.size >= MAX_PRE_CONNECT_FRAMES) {
                                preConnectQueue.poll()
                            }
                            preConnectQueue.offer(chunk)
                        }
                    }
                }
            } finally {
                // 在工作线程内安全停止并释放 AudioRecord，彻底避免跨线程并发 release 崩溃
                runCatching { recorder.stop() }
                runCatching { recorder.release() }
            }

            // 录音循环退出后，按严格顺序发送负序号尾包
            if (activeSessionId == mySessionId) {
                val ws = activeSocket.get() ?: currentWebSocket
                if (ws != null) {
                    runCatching {
                        if (isHandshakeDone.get()) drainPreConnectQueue(ws)
                        val seq = -sequenceCounter.getAndIncrement()
                        val endPacket = packBinaryPacket(
                            messageType = MSG_AUDIO_ONLY_REQUEST,
                            flags = FLAG_NEG_SEQUENCE,
                            sequence = seq,
                            payload = gzipCompress(ByteArray(0))
                        )
                        ws.send(endPacket.toByteString())
                    }
                }
            }
        }
    }

    /**
     * 停止推流并等待服务端尾包完成固化（最多等待 280ms）
     */
    fun stop(onCompleted: ((String) -> Unit)? = null) {
        val stoppingSessionId = activeSessionId
        if (!isRecording.getAndSet(false)) {
            onCompleted?.invoke(lastRecognizedText)
            return
        }

        pendingStopCallback = onCompleted

        // 超时兜底：若服务端在 280ms 内未返回尾包，使用已有最新文本固化
        mainHandler.postDelayed({
            deliverStopIfPending(stoppingSessionId, lastRecognizedText)
        }, TAIL_WAIT_TIMEOUT_MS)
    }

    private fun deliverStopIfPending(sessionId: Int, finalText: String): Boolean {
        if (sessionId != activeSessionId) return false
        if (!stopDelivered.compareAndSet(false, true)) return false

        val cb = pendingStopCallback
        pendingStopCallback = null
        val ws = currentWebSocket
        currentWebSocket = null

        mainHandler.post {
            cb?.invoke(finalText)
        }
        runCatching { ws?.close(1000, "Done") }
        return true
    }

    private fun flushPendingStop() {
        val sessionId = activeSessionId
        if (pendingStopCallback != null) {
            deliverStopIfPending(sessionId, lastRecognizedText)
        }
    }

    /** 取消当前录音与识别 */
    fun cancel() {
        activeSessionId = sessionGenerator.incrementAndGet()
        isRecording.set(false)
        pendingStopCallback = null
        stopDelivered.set(true)
        runCatching { currentWebSocket?.close(1000, "cancel") }
        currentWebSocket = null
        lastRecognizedText = ""
    }

    fun testConnection(
        apiKey: String,
        appId: String = "",
        cluster: String = "volc.seedasr.sauc.duration",
        onResult: (Boolean, String) -> Unit
    ) {
        if (apiKey.isBlank()) {
            mainHandler.post { onResult(false, "API Key 不能为空") }
            return
        }
        val reqId = UUID.randomUUID().toString()
        val request = buildWebSocketRequest(apiKey, appId, cluster, reqId)
        val finished = AtomicBoolean(false)

        fun notifyOnce(ok: Boolean, msg: String) {
            if (finished.compareAndSet(false, true)) {
                mainHandler.post { onResult(ok, msg) }
            }
        }

        okHttpClient.newWebSocket(request, object : WebSocketListener() {
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
                val data = bytes.toByteArray()
                if (data.size >= 2) {
                    val msgType = (data[1].toInt() shr 4) and 0x0F
                    if (msgType == MSG_ERROR_SERVER_RESPONSE) {
                        val (errCode, errMsg) = parseErrorResponse(data)
                        notifyOnce(false, "鉴权或配置错误 ($errCode): $errMsg")
                        runCatching { webSocket.close(1000, "test end") }
                        return
                    }
                    if (msgType == MSG_FULL_SERVER_RESPONSE || msgType == MSG_SERVER_ACK) {
                        val (code, _) = extractResponseJson(data) ?: Pair(1000, "")
                        if (code == 1000 || code == 20000000 || code == 0) {
                            notifyOnce(true, "连接成功！豆包流式大模型可用")
                        } else {
                            notifyOnce(false, "服务返回异常状态码: $code")
                        }
                        runCatching { webSocket.close(1000, "test ok") }
                    }
                }
            }

            override fun onFailure(webSocket: WebSocket, t: Throwable, response: Response?) {
                val code = response?.code
                val msg = when (code) {
                    401 -> "鉴权失败 (401)：API Key 无效"
                    403 -> "无权限 (403)：请确认已开通豆包流式识别服务"
                    400 -> "请求参数错误 (400)"
                    429 -> "请求过于频繁 (429)"
                    500 -> "服务端内部错误 (500)"
                    else -> "连接失败: ${t.message ?: "网络不可达"}"
                }
                notifyOnce(false, msg)
            }
        })
    }

    private fun parseServerResponse(sessionId: Int, data: ByteArray, callback: StreamCallback) {
        if (sessionId != activeSessionId) return
        var isFinalPacket = false
        if (data.size >= 2) {
            val msgType = (data[1].toInt() shr 4) and 0x0F
            val flags = data[1].toInt() and 0x0F
            isFinalPacket = (flags and 0x02) != 0
            if (msgType == MSG_SERVER_ACK) return
            if (msgType == MSG_ERROR_SERVER_RESPONSE) {
                val (errCode, errMsg) = parseErrorResponse(data)
                XLog.e("DoubaoStreamVoice: 服务端错误 code=$errCode msg=$errMsg", null)
                if (!deliverStopIfPending(sessionId, lastRecognizedText)) {
                    postError(callback, "识别服务报错 ($errCode): $errMsg")
                }
                return
            }
        }

        val (code, jsonStr) = extractResponseJson(data) ?: return
        if (code != 1000 && code != 20000000 && code != 0) {
            XLog.e("DoubaoStreamVoice: 错误码 code=$code raw=$jsonStr", null)
            if (!deliverStopIfPending(sessionId, lastRecognizedText)) {
                postError(callback, "识别服务提示错误 ($code)")
            }
            return
        }

        runCatching {
            val root = JSONObject(jsonStr)
            val resultObj = root.optJSONObject("result")
            val text = if (resultObj != null) {
                resultObj.optString("text").orEmpty()
            } else {
                val arr = root.optJSONArray("result")
                if (arr != null && arr.length() > 0) {
                    arr.optJSONObject(0)?.optString("text").orEmpty()
                } else {
                    root.optString("text").orEmpty()
                }
            }
            if (text.isNotBlank()) {
                lastRecognizedText = text
            }
            if (isFinalPacket || !isRecording.get()) {
                // 已处于松手收尾阶段或收到服务端最终尾包：直接完成唯一一次最终固化
                if (!deliverStopIfPending(sessionId, lastRecognizedText) && text.isNotBlank()) {
                    mainHandler.post {
                        if (sessionId == activeSessionId && !stopDelivered.get()) {
                            callback.onPartialText(text)
                        }
                    }
                }
            } else if (text.isNotBlank()) {
                mainHandler.post {
                    if (sessionId == activeSessionId && isRecording.get() && !stopDelivered.get()) {
                        callback.onPartialText(text)
                    }
                }
            }
        }.onFailure { e ->
            XLog.e("DoubaoStreamVoice: 解析响应 JSON 失败", e)
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

    private fun parseErrorResponse(data: ByteArray): Pair<Int, String> {
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
            val json = runCatching { JSONObject(jsonStr) }.getOrNull() ?: continue
            val code = if (json.has("code")) json.optInt("code", -1) else 1000
            return Pair(code, jsonStr)
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
