package io.openems.backend.timedata.nats;

import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;

@ObjectClassDefinition(//
		name = "Timedata.Nats", //
		description = "Taps the edge telemetry stream (with the original edge timestamps) and forwards it to a "
				+ "NATS JetStream broker. This is a read-only tap: it never answers historic queries and is designed "
				+ "to never block or break the InfluxDB ingestion path.")
@interface Config {

	@AttributeDefinition(name = "Component-ID", description = "Unique ID of this Component")
	String id() default "nats0";

	@AttributeDefinition(name = "NATS URL", description = "Broker URL, e.g. nats://host:4222 or tls://host:4222")
	String url() default "nats://localhost:4222";

	@AttributeDefinition(name = "Subject prefix", description = "Subject is built as '<prefix>.<edgeId>'. The stream "
			+ "should be configured with subjects '<prefix>.>'.")
	String subjectPrefix() default "openems.telemetry";

	@AttributeDefinition(name = "Username", description = "Username for broker authentication; leave empty for no auth")
	String username() default "";

	@AttributeDefinition(name = "Password", type = org.osgi.service.metatype.annotations.AttributeType.PASSWORD, //
			description = "Password for broker authentication")
	String password() default "";

	@AttributeDefinition(name = "Ensure stream", description = "If true, create the JetStream stream on first connect "
			+ "when it does not exist yet.")
	boolean ensureStream() default true;

	@AttributeDefinition(name = "Stream name", description = "JetStream stream name (used only when 'Ensure stream' "
			+ "is enabled)")
	String streamName() default "TELEMETRY";

	@AttributeDefinition(name = "Duplicate window (seconds)", description = "JetStream de-duplication window used when "
			+ "the stream is created by this component. Messages carry a Nats-Msg-Id so duplicates within this window "
			+ "are dropped. Note: correctness of energy counters does NOT depend on this (counter-diff is idempotent).")
	int duplicateWindowSeconds() default 120;

	@AttributeDefinition(name = "Forward aggregated data", description = "Also forward the 5-minute AggregatedData "
			+ "notifications (src=agg). Disable to forward only live + resend data.")
	boolean publishAggregated() default true;

	@AttributeDefinition(name = "Forward resend data", description = "Also forward ResendData notifications buffered "
			+ "by the edge during disconnects (src=resend, original timestamps preserved). Note: the InfluxDB provider "
			+ "currently ignores resend data, so enabling this makes the NATS stream a superset of InfluxDB.")
	boolean publishResend() default true;

	@AttributeDefinition(name = "Max in-flight publishes", description = "Upper bound of un-acked async publishes. "
			+ "Caps memory when the broker is slow.")
	int maxInflight() default 1000;

	@AttributeDefinition(name = "Queue size", description = "Bounded in-memory hand-off queue between the ingestion "
			+ "thread and the publisher worker. When full, messages are dropped (InfluxDB stays the source of truth).")
	int queueSize() default 100_000;

	@AttributeDefinition(name = "Connection timeout (seconds)", description = "Timeout for establishing the broker "
			+ "connection.")
	int connectionTimeoutSeconds() default 5;

	String webconsole_configurationFactory_nameHint() default "Timedata NATS JetStream [{id}]";

}
