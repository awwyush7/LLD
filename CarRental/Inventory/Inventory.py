from typing import List
from Vehicle.vehicle import Vehicle
from threading import Lock
class VehicleAlreadyPresent(Exception):
    pass

class Inventory:
    def __init__(self, location):
        self.location = location
        # self.vehicles : List[Vehicle] = []
        self.vehicles_available = {}
        self.lock = Lock()

    def get_vehicle(self):
        vehicles = []
        for vehicle_id, vehicle in self.vehicles_available.items():
            vehicles.append(vehicle_id)
        return vehicles

    def add_vehicle(self, vehicle : Vehicle):
        with self.lock:
            if vehicle in self.vehicles_available:
                raise VehicleAlreadyPresent
            else: 
                self.vehicles_available[vehicle.id] = vehicle

    def add_vehicles(self, vehicles : List[Vehicle]):
        with self.lock:
            for vehicle in vehicles:
                self.add_vehicle(vehicle)
    
