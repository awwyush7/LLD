
class Event:
    pass

class BookingEvent(Event):
    def __init__(self, action, **kwgs):
        self._vehicle_ids = kwgs[0]
        self._from_date = kwgs[1]
        self._to_date = kwgs[2]
        self._action = action
    
class PaymentEvent(Event):
    def __init__(self, amount):
        self._amount = amount