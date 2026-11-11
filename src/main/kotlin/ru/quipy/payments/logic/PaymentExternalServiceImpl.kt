package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.bucket4j.Bandwidth
import io.github.bucket4j.Bucket
import io.github.bucket4j.Refill
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.ConnectionPool
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry,
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val deadlineReserveMs = requestAverageProcessingTime.toMillis() + 500L
    private val admissionReserveMs = requestAverageProcessingTime.toMillis() * 2 + 200L
    private val workerThreads = minOf(
        parallelRequests.toLong(),
        rateLimitPerSec * requestAverageProcessingTime.toMillis() / 1000L + 2
    ).toInt()
    private val queueCapacity = 512
    private val pollTimeoutMs = 50L
    private val retryBackoffMs = 200L
    private val minEmissionGapMs = 1000L / rateLimitPerSec + 1

    private val emissionBucket: Bucket = Bucket.builder()
        .addLimit(
            Bandwidth.classic(1, Refill.greedy(rateLimitPerSec.toLong(), Duration.ofSeconds(1)))
        )
        .build()

    private data class PaymentTask(
        val paymentId: UUID,
        val amount: Int,
        val paymentStartedAt: Long,
        val deadline: Long,
    )

    private val taskQueue = LinkedBlockingQueue<PaymentTask>(queueCapacity)

    private val attemptExecutor = ThreadPoolExecutor(
        workerThreads,
        workerThreads,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(),
        NamedThreadFactory("payment-worker-$accountName")
    )

    init {
        NamedThreadFactory("payment-dispatcher-$accountName").newThread { dispatcherLoop() }.start()
    }

    private fun acquireEmissionSlot(deadline: Long): Boolean {
        val maxWaitMs = deadline - System.currentTimeMillis() - deadlineReserveMs
        return maxWaitMs > 0 && emissionBucket.asBlocking()
            .tryConsume(1, Duration.ofMillis(maxWaitMs))
    }

    private val client = OkHttpClient.Builder()
        .connectTimeout(5, TimeUnit.SECONDS)
        .readTimeout(15, TimeUnit.SECONDS)
        .connectionPool(ConnectionPool(workerThreads, 60, TimeUnit.SECONDS))
        .build()

    override fun canAcceptByDeadline(deadline: Long): Boolean =
        System.currentTimeMillis() + taskQueue.size * minEmissionGapMs + admissionReserveMs <= deadline

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        if (!taskQueue.offer(PaymentTask(paymentId, amount, paymentStartedAt, deadline))) {
            decline("queue-full")
        }
    }

    private fun decline(reason: String) {
        meterRegistry.counter("payments_declined", "account", accountName, "reason", reason).increment()
    }

    private fun dispatcherLoop() {
        while (true) {
            try {
                val task = taskQueue.poll(pollTimeoutMs, TimeUnit.MILLISECONDS) ?: continue
                if (System.currentTimeMillis() + deadlineReserveMs > task.deadline) {
                    decline("deadline-exceeded")
                    continue
                }
                if (!acquireEmissionSlot(task.deadline)) {
                    decline("deadline-exceeded")
                    continue
                }
                attemptExecutor.execute { processPayment(task) }
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                return
            } catch (e: Exception) {
                logger.error("[$accountName] dispatcher loop error", e)
            }
        }
    }

    private fun processPayment(task: PaymentTask) {
        var backoff = retryBackoffMs
        if (performPaymentAttempt(task.paymentId, task.amount, task.paymentStartedAt)) {
            return
        }
        while (System.currentTimeMillis() + deadlineReserveMs <= task.deadline) {
            Thread.sleep(backoff)
            backoff *= 2
            if (!acquireEmissionSlot(task.deadline)) {
                break
            }
            if (performPaymentAttempt(task.paymentId, task.amount, task.paymentStartedAt)) {
                return
            }
        }
        decline("deadline-exceeded")
    }

    private fun performPaymentAttempt(paymentId: UUID, amount: Int, paymentStartedAt: Long): Boolean {
        meterRegistry.counter("payment_attempts", "account", accountName).increment()
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        var result = false
        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            client.newCall(request).execute().use { response ->
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                result = body.result

                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                paymentESService.update(paymentId) {
                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
        return result
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()