class Bill:
    def __init__(self, id, reservation_id, amount, payment_provider):
        self.__id = id
        self.__reservation_id = reservation_id
        self.__amount = amount
        self.__payment_provider = payment_provider

    def get_id(self):
        return self.__id
    
    def get_reservation_id(self):
        return self.__reservation_id
    
    def get_amount(self):
        return self.__amount
    
    def get_payment_provider(self):
        return self.__payment_provider