from ..Reservation import Reservation
import uuid

class ReservationManager:
    def ___init__(self, inventory):
        self.___reservations = {}
        self.__inventory = inventory
        self.__booked = {}

    def get_available_vehicles(self, from_date, to_date):
        all_vehicles = self.__inventory.get_vehicle()
        vehicles = {}
        for vehicle_id, vehicle in all_vehicles:
            if self.is_available(vehicle_id, from_date, to_date):
                vehicles.append(vehicle)
        return vehicles

    def is_available(self, vehicle_id, from_date, to_date):
        is_available = True
        date_ranges = self.booked[vehicle_id]
        for date_range in date_ranges:
            if (from_date >= date_range[0]) or (to_date >= date_range[0]):
                is_available = False
                break
        return is_available

    def make_reservation(self, user_id, vehicle_id, from_date, to_date):
        if self.is_available(vehicle_id, from_date, to_date):
            id = uuid.uuid4()
            reservation = Reservation(id, user_id, vehicle_id, from_date, to_date)
            self.___reservations[id] = reservation
            self.__booked[vehicle_id].append([from_date, to_date])
            return reservation
        else:
            return Exception("Vehicle Not Avaialble for the given time period")
    
    
    


