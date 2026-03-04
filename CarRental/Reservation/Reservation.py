from typing import Dict
from reservation_status import ReservationStatus

class Reservation:
    def __init__(self, id, user_id, vehicle_id, from_date, to_date):
        self.__id = id
        self.__user_id = user_id
        self.__vehilce_id = vehicle_id
        self.__from_date = from_date
        self.__to_date = to_date
        self.__payment_status = ReservationStatus.STARTED

    def change_status(self, new_status, payment_id):
        if(self.__payment_status == ReservationStatus.STARTED and payment_id != ""):
            self.__payment_status = ReservationStatus.COMPLETED
            self.__payment_id = payment_id
        elif (self.__payment_status == ReservationStatus.STARTED and payment_id == ""):
            self.__payment_status = ReservationStatus.FAILED

    def get_id(self):
        return self.__id