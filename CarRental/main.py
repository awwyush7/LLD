from CarRentalSystem.car_rental_system import CarRentalSystem
from Inventory.Inventory import Inventory
from ReservationManager.reservation_manager import ReservationManager
from Payment.PaymentManager.payment_manager import PaymentManager

inventory = Inventory("Lucknow")
reservation_manager = ReservationManager(inventory)
payment_manager = PaymentManager("Stripe")


car_rental_system = CarRentalSystem(inventory, reservation_manager, payment_manager)

car_rental_system.get_available_vehicles("2024-06-01", "2024-06-10")
bill = car_rental_system.make_reservation("user1", "vehicle1", "2024-06-01", "2024-06-10")

print(bill)