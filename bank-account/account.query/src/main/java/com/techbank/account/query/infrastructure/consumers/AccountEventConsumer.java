package com.techbank.account.query.infrastructure.consumers;

import com.techbank.account.common.events.AccountClosedEvent;
import com.techbank.account.common.events.AccountOpenedEvent;
import com.techbank.account.common.events.FundsDepositedEvent;
import com.techbank.account.common.events.FundsWithdrawnEvent;
import com.techbank.account.query.infrastructure.handlers.EventHandler;
import com.techbank.cqrs.core.events.BaseEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.text.MessageFormat;
import java.util.logging.Level;
import java.util.logging.Logger;

@Service
public class AccountEventConsumer implements EventConsumer {
    private final Logger logger = Logger.getLogger(AccountEventConsumer.class.getName());

    @Autowired
    private EventHandler eventHandler;

    @KafkaListener(topics = "${app.kafka.topic}", groupId = "${spring.kafka.consumer.group-id}")
    @Override
    public void consume(@Payload BaseEvent event, Acknowledgment ack) {
        logger.log(
                Level.INFO,
                MessageFormat.format(
                        "Consumed event {0} for accountId={1}, version={2}",
                        event.getClass().getSimpleName(),
                        event.getId(),
                        event.getVersion()
                )
        );
        if (event instanceof AccountOpenedEvent) {
            this.eventHandler.on((AccountOpenedEvent) event);
        } else if (event instanceof FundsDepositedEvent) {
            this.eventHandler.on((FundsDepositedEvent) event);
        } else if (event instanceof FundsWithdrawnEvent) {
            this.eventHandler.on((FundsWithdrawnEvent) event);
        } else if (event instanceof AccountClosedEvent) {
            this.eventHandler.on((AccountClosedEvent) event);
        } else {
            logger.log(Level.WARNING, MessageFormat.format("Unsupported event type received: {0}", event.getClass().getName()));
        }
        ack.acknowledge();
    }
}
