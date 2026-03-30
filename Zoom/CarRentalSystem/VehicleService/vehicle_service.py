from Zoom.CarRentalSystem.VehicleService.Vehicle.vehicle import Vehicle

class VehicleService:
    def __init__(self):
        self._vehicles = {}

    def add_vehicle(self, vehicle : Vehicle):
        self._vehicles[vehicle.id] = vehicle

    def remove_vehicle(self, vehicle_id):
        self._vehicles.remove(vehicle_id)
