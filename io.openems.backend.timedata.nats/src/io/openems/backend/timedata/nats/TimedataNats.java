package io.openems.backend.timedata.nats;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.metatype.annotations.Designate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import io.nats.client.Connection;
import io.nats.client.Connection.Status;
import io.nats.client.JetStream;
import io.nats.client.Nats;
import io.nats.client.Options;
import io.nats.client.PublishOptions;
import io.nats.client.api.PublishAck;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.openems.backend.common.component.AbstractOpenemsBackendComponent;
import io.openems.backend.common.debugcycle.DebugLoggable;
import io.openems.backend.common.timedata.Timedata;
import io.openems.common.exceptions.OpenemsError.OpenemsNamedException;
import io.openems.common.jsonrpc.notification.AbstractDataNotification;
import io.openems.common.jsonrpc.notification.AggregatedDataNotification;
import io.openems.common.jsonrpc.notification.ResendDataNotification;
import io.openems.common.jsonrpc.notification.TimestampedDataNotification;
import io.openems.common.timedata.Resolution;
import io.openems.common.types.ChannelAddress;

/**
 * Read-only Timedata tap that forwards the raw edge telemetry stream - keeping
 * the original edge timestamps - to a NATS JetStream broker.
 *
 * <p>
 * Safety contract (so this never disturbs the InfluxDB ingestion path, which
 * runs in the SAME sequential fan-out loop on the notification thread):
 * <ul>
 * <li>{@code write(...)} never throws (every error is swallowed),
 * <li>{@code write(...)} never blocks (it only serializes + {@code offer()}s to
 * a bounded queue and returns immediately),
 * <li>{@code write(...)} only reads the shared notification data, never mutates
 * it,
 * <li>all query methods return {@code null} (tap-only - never answers historic
 * queries, never shadows InfluxDB).
 * </ul>
 * Configure this provider AFTER InfluxDB in {@code Core.TimedataManager} so it
 * can never pre-empt the InfluxDB write.
 */
@Designate(ocd = Config.class, factory = true)
@Component(//
		name = "Timedata.Nats", //
		configurationPolicy = ConfigurationPolicy.REQUIRE, //
		immediate = true //
)
public class TimedataNats extends AbstractOpenemsBackendComponent implements Timedata, DebugLoggable {

	private static final Gson GSON = new Gson();
	private static final String SRC_LIVE = "live";
	private static final String SRC_AGGREGATED = "agg";
	private static final String SRC_RESEND = "resend";
	private static final long RECONNECT_BACKOFF_MS = 5_000;

	private final Logger log = LoggerFactory.getLogger(TimedataNats.class);

	// Metrics (best-effort, for observability of a lossy tap).
	private final AtomicLong publishedCount = new AtomicLong();
	private final AtomicLong droppedCount = new AtomicLong();
	private final AtomicLong publishFailedCount = new AtomicLong();
	private final AtomicLong serializeErrorCount = new AtomicLong();

	private Config config;
	private BlockingQueue<PendingMessage> queue;
	private Semaphore inflight;
	private volatile boolean running;
	private Thread worker;

	// Owned and mutated exclusively by the worker thread.
	private Connection connection;
	private JetStream jetStream;
	private long lastConnectErrorLog;

	public TimedataNats() {
		super("Timedata.Nats");
	}

	private record PendingMessage(String subject, String messageId, byte[] payload) {
	}

	@Activate
	private void activate(Config config) {
		this.config = config;
		this.queue = new ArrayBlockingQueue<>(Math.max(1, config.queueSize()));
		this.inflight = new Semaphore(Math.max(1, config.maxInflight()));
		this.running = true;
		this.worker = new Thread(this::runWorker, "Timedata.Nats-" + config.id());
		this.worker.setDaemon(true);
		this.worker.start();
		final var auth = config.username() != null && !config.username().isEmpty() ? "USER_PASS" : "NONE";
		this.logInfo(this.log, "Activate [id=" + config.id() + ";url=" + config.url() + ";subjectPrefix="
				+ config.subjectPrefix() + ";auth=" + auth + "]");
	}

	@Deactivate
	private void deactivate() {
		this.logInfo(this.log, "Deactivate");
		this.running = false;
		final var w = this.worker;
		if (w != null) {
			w.interrupt();
			try {
				w.join(Duration.ofSeconds(6).toMillis());
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}
		this.worker = null;
	}

	// --- Timedata write methods: only ever serialize + offer, never throw/block. ---

	@Override
	public void write(String edgeId, TimestampedDataNotification data) {
		this.enqueue(edgeId, data, SRC_LIVE);
	}

	@Override
	public void write(String edgeId, AggregatedDataNotification data) {
		if (!this.config.publishAggregated()) {
			return;
		}
		this.enqueue(edgeId, data, SRC_AGGREGATED);
	}

	@Override
	public void write(String edgeId, ResendDataNotification data) {
		if (!this.config.publishResend()) {
			return;
		}
		this.enqueue(edgeId, data, SRC_RESEND);
	}

	/**
	 * Converts a notification into one message per timestamp row and offers them to
	 * the bounded hand-off queue. Runs on the ingestion thread - must be fast,
	 * non-blocking and must never propagate an exception to the fan-out loop.
	 *
	 * @param edgeId       the Edge-ID
	 * @param notification the {@link AbstractDataNotification}
	 * @param src          the source tag ({@link #SRC_LIVE}/{@link #SRC_AGGREGATED}/
	 *                     {@link #SRC_RESEND})
	 */
	private void enqueue(String edgeId, AbstractDataNotification notification, String src) {
		try {
			if (notification == null) {
				return;
			}
			final var subject = this.config.subjectPrefix() + "." + sanitizeSubjectToken(edgeId);
			// rowMap(): timestamp(ms) -> (channelAddress -> value). Read-only access.
			for (var row : notification.getData().rowMap().entrySet()) {
				final var timestamp = row.getKey();
				final var channels = row.getValue();
				if (channels == null || channels.isEmpty()) {
					continue;
				}

				final var payload = new JsonObject();
				payload.addProperty("v", 1);
				payload.addProperty("edgeId", edgeId);
				payload.addProperty("timestamp", timestamp);
				payload.addProperty("src", src);
				final var channelObject = new JsonObject();
				for (var channel : channels.entrySet()) {
					channelObject.add(channel.getKey(), channel.getValue());
				}
				payload.add("channels", channelObject);

				final var bytes = GSON.toJson(payload).getBytes(StandardCharsets.UTF_8);
				// De-dup namespace: resend re-sends data already sent live -> share the live
				// id so JetStream drops it within the duplicate window. Aggregated data is a
				// different resolution -> separate id namespace to avoid false de-dup.
				final var messageId = SRC_AGGREGATED.equals(src) //
						? edgeId + ":agg:" + timestamp //
						: edgeId + ":" + timestamp;

				if (!this.queue.offer(new PendingMessage(subject, messageId, bytes))) {
					this.droppedCount.incrementAndGet();
				}
			}
		} catch (Throwable t) {
			// Must never break the TimedataManager fan-out loop (InfluxDB runs in it too).
			this.serializeErrorCount.incrementAndGet();
		}
	}

	// --- Publisher worker (own thread; here blocking/waiting is fine). ---

	private void runWorker() {
		while (this.running) {
			try {
				if (!this.ensureConnected()) {
					Thread.sleep(RECONNECT_BACKOFF_MS);
					continue;
				}
				final var msg = this.queue.poll(1, TimeUnit.SECONDS);
				if (msg == null) {
					continue;
				}
				this.publish(msg);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			} catch (Throwable t) {
				// keep the worker alive no matter what
				this.publishFailedCount.incrementAndGet();
			}
		}
		this.closeConnection();
	}

	private void publish(PendingMessage msg) throws InterruptedException {
		final var js = this.jetStream;
		if (js == null) {
			this.droppedCount.incrementAndGet();
			return;
		}
		final var options = PublishOptions.builder().messageId(msg.messageId()).build();
		var acquired = false;
		try {
			this.inflight.acquire();
			acquired = true;
			final CompletableFuture<PublishAck> future = js.publishAsync(msg.subject(), msg.payload(), options);
			acquired = false; // ownership of the permit transferred to the completion callback
			future.whenComplete((ack, ex) -> {
				this.inflight.release();
				if (ex != null) {
					this.publishFailedCount.incrementAndGet();
				} else {
					this.publishedCount.incrementAndGet();
				}
			});
		} catch (InterruptedException e) {
			if (acquired) {
				this.inflight.release();
			}
			throw e;
		} catch (Throwable t) {
			// publishAsync failed synchronously (e.g. connection lost) -> drop the message
			this.publishFailedCount.incrementAndGet();
		} finally {
			if (acquired) {
				this.inflight.release();
			}
		}
	}

	/**
	 * Ensures a usable JetStream connection. Creates a new connection on demand;
	 * relies on jnats' built-in auto-reconnect once a connection exists. Never
	 * throws - returns {@code false} when the broker is currently unreachable.
	 *
	 * @return true if connected and ready to publish
	 */
	private boolean ensureConnected() {
		final var current = this.connection;
		if (current != null) {
			final var status = current.getStatus();
			if (status == Status.CONNECTED) {
				return true;
			}
			if (status == Status.CONNECTING || status == Status.RECONNECTING) {
				return false; // let jnats finish (re)connecting; meanwhile the queue drops on overflow
			}
			// CLOSED / DISCONNECTED -> discard and rebuild
			this.closeConnection();
		}

		try {
			final var connection = Nats.connect(this.buildOptions());
			this.jetStream = connection.jetStream();
			if (this.config.ensureStream()) {
				this.ensureStream(connection);
			}
			this.connection = connection;
			this.logInfo(this.log, "Connected to NATS [" + this.config.url() + "]");
			return true;
		} catch (Throwable t) {
			// rate-limit the error log to once per backoff window
			final var now = System.currentTimeMillis();
			if (now - this.lastConnectErrorLog > RECONNECT_BACKOFF_MS) {
				this.lastConnectErrorLog = now;
				this.logWarn(this.log, "Unable to connect to NATS [" + this.config.url() + "]: " + t.getMessage());
			}
			return false;
		}
	}

	private Options buildOptions() {
		final var builder = new Options.Builder() //
				.server(this.config.url()) //
				.connectionTimeout(Duration.ofSeconds(Math.max(1, this.config.connectionTimeoutSeconds()))) //
				.maxReconnects(-1) // reconnect forever
				.connectionName("openems-" + this.config.id());

		final var username = this.config.username();
		if (username != null && !username.isEmpty()) {
			builder.userInfo(username, this.config.password());
		}
		return builder.build();
	}

	/**
	 * Idempotently ensures the JetStream stream exists. Only invoked when
	 * {@code ensureStream} is enabled; otherwise the stream is expected to be
	 * pre-provisioned.
	 *
	 * @param connection the live NATS {@link Connection}
	 * @throws Exception on connection / API error (treated as connect failure)
	 */
	private void ensureStream(Connection connection) throws Exception {
		final var jsm = connection.jetStreamManagement();
		try {
			jsm.getStreamInfo(this.config.streamName());
			return; // already exists
		} catch (Exception e) {
			// not found (or transient) -> attempt to create below
		}
		final var streamConfig = StreamConfiguration.builder() //
				.name(this.config.streamName()) //
				.subjects(this.config.subjectPrefix() + ".>") //
				.storageType(StorageType.File) //
				.duplicateWindow(Duration.ofSeconds(Math.max(0, this.config.duplicateWindowSeconds()))) //
				.build();
		jsm.addStream(streamConfig);
		this.logInfo(this.log, "Created JetStream stream [" + this.config.streamName() + "] subjects ["
				+ this.config.subjectPrefix() + ".>]");
	}

	private void closeConnection() {
		final var current = this.connection;
		this.connection = null;
		this.jetStream = null;
		if (current != null) {
			try {
				// flush pending acks, then close
				current.drain(Duration.ofSeconds(5));
			} catch (Exception e) {
				try {
					current.close();
				} catch (Exception ignore) {
					// nothing more we can do
				}
			}
		}
	}

	private static String sanitizeSubjectToken(String token) {
		if (token == null || token.isEmpty()) {
			return "unknown";
		}
		// NATS subject tokens must not contain spaces or the special chars . * >
		return token.replaceAll("[.\\s*>]", "_");
	}

	// --- Tap-only: never answer queries (do not shadow InfluxDB). ---

	@Override
	public SortedMap<ZonedDateTime, SortedMap<ChannelAddress, JsonElement>> queryHistoricData(String edgeId,
			ZonedDateTime fromDate, ZonedDateTime toDate, Set<ChannelAddress> channels, Resolution resolution)
			throws OpenemsNamedException {
		return null;
	}

	@Override
	public SortedMap<ChannelAddress, JsonElement> queryHistoricEnergy(String edgeId, ZonedDateTime fromDate,
			ZonedDateTime toDate, Set<ChannelAddress> channels) throws OpenemsNamedException {
		return null;
	}

	@Override
	public SortedMap<ZonedDateTime, SortedMap<ChannelAddress, JsonElement>> queryHistoricEnergyPerPeriod(String edgeId,
			ZonedDateTime fromDate, ZonedDateTime toDate, Set<ChannelAddress> channels, Resolution resolution)
			throws OpenemsNamedException {
		return null;
	}

	@Override
	public String id() {
		return this.config.id();
	}

	@Override
	public String debugLog() {
		final var c = this.connection;
		final var status = c == null ? "DISCONNECTED" : c.getStatus().toString();
		return "[" + this.getName() + "] " + this.config.id() //
				+ " status=" + status //
				+ " queue=" + this.queue.size() //
				+ " published=" + this.publishedCount.get() //
				+ " dropped=" + this.droppedCount.get() //
				+ " failed=" + this.publishFailedCount.get();
	}

	@Override
	public Map<String, JsonElement> debugMetrics() {
		final var metrics = new JsonObject();
		metrics.addProperty("queueSize", this.queue.size());
		metrics.addProperty("published", this.publishedCount.get());
		metrics.addProperty("dropped", this.droppedCount.get());
		metrics.addProperty("publishFailed", this.publishFailedCount.get());
		metrics.addProperty("serializeError", this.serializeErrorCount.get());
		return metrics.asMap();
	}

}
