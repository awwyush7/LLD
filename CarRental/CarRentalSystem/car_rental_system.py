from Inventory.inventory import Inventory
from ReservationManager.reservation_manager import ReservationManager
from Payment.PaymentManager.payment_manager import PaymentManager
from datetime import datetime  # or datetime, depending on your use case

class CarRentalSystem:
    def __init__(self, inventory: Inventory, reservation_manager: ReservationManager, payment_manager: PaymentManager):
        self.inventory = inventory
        self.reservation_manager = reservation_manager 
        self.payment_manager = payment_manager
    
    def get_available_vehicles(self, from_date: datetime, to_date: datetime):  # Added type annotations
        return self.reservation_manager.get_available_vehicles(from_date, to_date)
    
    def make_reservation(self, user_id: str, vehicle_id: str, from_date: datetime, to_date: datetime):  # Added type annotations
        try:
            reservation = self.reservation_manager.make_reservation(user_id, vehicle_id, from_date, to_date)
            bill = self.payment_manager.make_payment(reservation)
            reservation.change_status(reservation.get_id(), bill.get_id())
            return reservation
        except Exception as e:
            print(e)
            return None
