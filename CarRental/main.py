from CarRentalSystem.car_rental_system import CarRentalSystem
from Inventory.inventory import Inventory
from ReservationManager.reservation_manager import ReservationManager
from Payment.PaymentManager.payment_manager import PaymentManager
from Vehicle.vehicle_conditions import VehicleCondidtion
from Vehicle.vehicle_state import VehicleState
from Vehicle.vehicle import Vehicle

inventory = Inventory("Ayodhya")
inventory.add_vehicle(Vehicle(12345, "car", "model1", 2016, VehicleCondidtion.HIGH, VehicleState.AVAILABLE, 90))
inventory.add_vehicle(Vehicle(12346, "car", "model2", 2010, VehicleCondidtion.LOW, VehicleState.OUTOFSERVICE, 70))
reservation_manager = ReservationManager(inventory)
payment_manager = PaymentManager("Stripe")


car_rental_system = CarRentalSystem(inventory, reservation_manager, payment_manager)

available = car_rental_system.get_available_vehicles("2024-06-01", "2024-06-10")
print(available)
bill = car_rental_system.make_reservation("user1", "vehicle1", "2024-06-01", "2024-06-10")

print(bill)

    