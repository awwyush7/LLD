class BookingService:
    def __init__(self, vehicle_service):
        self._vehicle_service = vehicle_service
        self._bookings = {}
    
    def book_vehicles(self, vehicle_ids : list, from_date, to_date):
        sorted_ids = vehicle_ids.sort()
        try:
                for vehicle_id in sorted_ids:
                    vehicle = self._vehicle_service[vehicle_id]
                    if not vehicle.lock.acquire(blocking = False):
                        return False
                    
                if not self.vehicles_available(vehicle_ids, from_date, to_date):
                    return False
                
                for vehicle_id in sorted_ids:
                    for i in range(from_date, to_date + 1):
                        self._bookings[vehicle_id][i] = 1
        finally:
            for vehicle_id in reversed(sorted_ids):
                vehicle = self._vehicle_service[vehicle_id]
                vehicle.lock.release()
        
    def process(self, message):
        message_dict =  message

    def is_available(self, vehicle_id, from_date, to_date):
        for i in range(from_date, to_date + 1):
            if self._bookings[vehicle_id][i] == 1:
                return False
        return True
    
    def vehicles_available(self, vehicle_ids, from_date, to_date):
        for vehicle_id in vehicle_ids:
            if self.is_available(vehicle_id, from_date, to_date) == False:
                return False
        return True
    
    def remove_booking(self, vehicle_ids, from_date, to_date):
        sorted_ids = vehicle_ids.sort()
        try:
            for vehicle_id in sorted_ids:
                vehicle = self._vehicle_service[vehicle_id]
                if not vehicle.lock.acquire(blocking = False):
                    return False
                
            if not self.vehicles_available(vehicle_ids, from_date, to_date):
                return False
            
            for vehicle_id in sorted_ids:
                for i in range(from_date, to_date + 1):
                    self._bookings[vehicle_id][i] = 0
        finally:
            for vehicle_id in reversed(sorted_ids):
                vehicle = self._vehicle_service[vehicle_id]
                vehicle.lock.release()


