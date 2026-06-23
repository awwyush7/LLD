from enum import Enum

class Topic(Enum):
    BookingTopic    = "BookingTopic"
    PaymentTopic    = "PaymentTopic"
    TicketTopic     = "TickeetTopic"   # typo kept intentionally — matches existing Kafka topic name

    # Dead Letter Queues — messages land here after MAX_RETRIES failures.
    # A partition-blocking poison pill gets moved here so the live topic drains.
    # Ops can inspect, fix, and replay DLQ messages without touching the main flow.
    BookingTopicDLQ = "BookingTopic-DLQ"
    PaymentTopicDLQ = "PaymentTopic-DLQ"
    TicketTopicDLQ  = "TicketTopic-DLQ"
