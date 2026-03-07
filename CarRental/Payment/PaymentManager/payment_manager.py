from Payment.Bill.bill import Bill

class PaymentManager:
    def __init__(self, payment_provider):
        self.__payment_provider = payment_provider
    
    def process_payment(self, reservation, amount):
        # Logic to process payment for the given reservation and amount
        try:
            print(f"Processing payment of {amount} for reservation {reservation.get_id()} using {self.__payment_provider}")
            result = self.__payment_provider.process(amount)
            if result.status != "success":
                raise Exception("Payment Failed")
            id = reservation.get_id() + "_payment"
            bill = Bill(id, reservation.get_id(),amount, self.__payment_provider)
            return result
        except Exception as e:
            print(e)
            return None