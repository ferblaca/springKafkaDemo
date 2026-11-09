package com.example.demoIdempotence;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.*;
import org.apache.kafka.common.errors.InvalidPidMappingException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

class InvalidPidMappingProducerFactoryTest {

	@Test
	void invalidPidMappingDoesNotAutomaticallyRecreateTheCachedProducer() {
		final RecordingProducerFactory factory = new RecordingProducerFactory();
		final Producer<String, String> firstProducer = factory.createProducer();
		final AtomicReference<Exception> sendFailure = new AtomicReference<>();

		firstProducer.send(new ProducerRecord<>("demo-topic", "value"),
				(metadata, exception) -> sendFailure.set(exception));

		assertThat(sendFailure.get()).isInstanceOf(InvalidPidMappingException.class);

		final Producer<String, String> producerAfterFailure = factory.createProducer();

		assertThat(producerAfterFailure).as("the cached producer is reused after InvalidPidMappingException")
				.isSameAs(firstProducer);
		assertThat(factory.createdProducers()).isOne();
		assertThat(factory.producers().getFirst().closeCalls()).isZero();
	}

	@Test
	void resetClosesTheFailedProducerAndTheNextSendUsesANewProducer() {
		final RecordingProducerFactory factory = new RecordingProducerFactory();
		final Producer<String, String> firstProducer = factory.createProducer();

		firstProducer.send(new ProducerRecord<>("demo-topic", "value"), (metadata, exception) -> {
		});
		factory.reset();

		final Producer<String, String> producerAfterReset = factory.createProducer();

		assertThat(producerAfterReset).isNotSameAs(firstProducer);
		assertThat(factory.createdProducers()).isEqualTo(2);
		assertThat(factory.producers().getFirst().closeCalls()).isEqualTo(1);
	}

	private static final class RecordingProducerFactory extends DefaultKafkaProducerFactory<String, String> {

		private final AtomicInteger producerSequence = new AtomicInteger();

		private final java.util.List<RecordingProducer> producers = new java.util.ArrayList<>();

		RecordingProducerFactory() {
			super(new HashMap<>());
			this.updateConfigs(java.util.Map.of("key.serializer", StringSerializer.class, "value.serializer",
					ByteArraySerializer.class, "enable.idempotence", true));
		}

		@Override
		protected Producer<String, String> createRawProducer(final java.util.Map<String, Object> configs) {
			final RecordingProducer producer = new RecordingProducer(this.producerSequence.incrementAndGet());
			this.producers.add(producer);
			return producer;
		}

		int createdProducers() {
			return this.producers.size();
		}

		java.util.List<RecordingProducer> producers() {
			return this.producers;
		}
	}

	private static final class RecordingProducer implements Producer<String, String> {

		private final int id;

		private int closeCalls;

		RecordingProducer(final int id) {
			this.id = id;
		}

		@Override
		public CompletableFuture<RecordMetadata> send(final ProducerRecord<String, String> record) {
			return this.send(record, null);
		}

		@Override
		public CompletableFuture<RecordMetadata> send(final ProducerRecord<String, String> record,
				final Callback callback) {
			final InvalidPidMappingException failure = new InvalidPidMappingException(
					"The producer attempted to use a producer id which is not currently assigned to its transactional id.");
			if (callback != null) {
				callback.onCompletion(null, failure);
			}
			return CompletableFuture.failedFuture(failure);
		}

		@Override
		public void flush() {
		}

		@Override
		public java.util.List<PartitionInfo> partitionsFor(final String topic) {
			return java.util.List.of();
		}

		@Override
		public java.util.Map<MetricName, ? extends Metric> metrics() {
			return java.util.Map.of();
		}

		@Override
		public void close() {
			this.closeCalls++;
		}

		@Override
		public void close(final Duration timeout) {
			this.closeCalls++;
		}

		int closeCalls() {
			return this.closeCalls;
		}

		@Override
		public Uuid clientInstanceId(final Duration timeout) {
			return Uuid.ZERO_UUID;
		}

		@Override
		public void registerMetricForSubscription(final KafkaMetric metric) {
		}

		@Override
		public void unregisterMetricFromSubscription(final KafkaMetric metric) {
		}

		@Override
		public void initTransactions() {
		}

		@Override
		public void beginTransaction() {
		}

		@Override
		public void sendOffsetsToTransaction(
				final java.util.Map<TopicPartition, org.apache.kafka.clients.consumer.OffsetAndMetadata> offsets,
				final org.apache.kafka.clients.consumer.ConsumerGroupMetadata groupMetadata) {
		}

		@Override
		public void commitTransaction() {
		}

		@Override
		public void abortTransaction() {
		}

	}
}
