package demo.idempotentconsumer.service;

import demo.idempotentconsumer.model.event.inbound.InboundEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class AsyncOrderService {;

    private OrderService orderService;

    @Autowired
    public AsyncOrderService(
            OrderService orderService
    ) {
        this.orderService = orderService;
    }

    @Async
    public void processNonIdempotentAsync(
            String key,
            InboundEvent event,
            boolean failDatabase
    ) {
        orderService.processNonIdempotent(key, event, failDatabase, true);
    }

    @Async
    public void processIdempotentAsync(
            String eventId,
            String key,
            InboundEvent event,
            boolean failDatabase
    ) {
        orderService.processIdempotent(eventId, key, event, failDatabase, true);
    }

    @Async
    public void processIdempotentAndOutboxAsync(
            String eventId,
            String key,
            InboundEvent event,
            boolean failDatabase
    ) {
        orderService.processIdempotentAndOutbox(eventId, event, failDatabase, true);
    }

}
