package com.exemplo.pagamentos.consumer;

import com.exemplo.pagamentos.domain.PagamentoEvent;
import com.exemplo.pagamentos.processing.InboxProcessor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class PagamentoListener {

    private final InboxProcessor processor;

    public PagamentoListener(InboxProcessor processor) {
        this.processor = processor;
    }

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${app.kafka.group-id}")
    public void onMessage(ConsumerRecord<String, PagamentoEvent> record, Acknowledgment ack) {
        processor.handle(record.value());
        ack.acknowledge();
    }
}
