from Zoom.CarRentalSystem.VehicleService.Vehicle.vehicle import Vehicle

class VehicleService:
    def __init__(self):
        self._vehicles = {}

    def add_vehicle(self, vehicle : Vehicle):
        self._vehicles[vehicle.id] = vehicle

    def get(self, vehicle_id):
        return self._vehicles.get(vehicle_id)
    
    def remove_vehicle(self, vehicle_id):
        self._vehicles.pop(vehicle_id,None)
