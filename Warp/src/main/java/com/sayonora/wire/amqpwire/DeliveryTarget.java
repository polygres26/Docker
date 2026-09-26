package com.sayonora.wire.amqpwire;

/** Where the broker's dispatcher hands claimed messages: a 0-9-1 channel or an AMQP 1.0 sender link. */
interface DeliveryTarget {

    /** Delivers one leased message; false when the consumer is gone (the message is then requeued). */
    boolean deliver(AmqpBroker.Consumer c, AmqpStore.MsgRow m);

    /** How many more messages the consumer can take right now (prefetch window / link credit). */
    int available(AmqpBroker.Consumer c);

    /** The queue was deleted under the consumer. */
    void serverCancel(AmqpBroker.Consumer c);
}
