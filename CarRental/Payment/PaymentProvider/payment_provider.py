from abc import ABC, abstractmethod

class PaymentProvider(ABC):
    def __init__(self, name : str):
        self.__name = name
    @abstractmethod
    def process_payment(self, amount : float) -> bool:
        pass

class StripePaymentProvider(PaymentProvider):
    def __init__(self):
        super().__init__("Stripe")
    
    def process_payment(self, amount : float) -> bool:
        print(f"Processing payment of {amount} using Stripe")
        return True
    

class UPIPaymentPrvider(PaymentProvider):
    def __init__(self):
        super().__init__("UPI")
    
    def process_payment(self, amount : float) -> bool:
        print(f"Processing payment of {amount} using UPI")
        return True