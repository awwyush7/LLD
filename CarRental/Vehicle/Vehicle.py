from vehicle_conditions import VehicleCondidtion
from vehicle_state import VehicleState

class Vehicle():
    def __init__(self, registration_number, type, model, make_year, condidtion : VehicleCondidtion, state : VehicleState, rental_price):
        self.registration_number = registration_number
        self.type = type
        self.model = model
        self.make_year = make_year
        self.condition = condidtion
        self.rental_price = rental_price
        self.state = state
    
    def change_condition(self, new_condition: VehicleCondidtion):
        self.condition = new_condition

    def change_state(self, new_state:VehicleState):
        self.state = new_state
