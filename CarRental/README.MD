
Requirements:
1) We can register different Vehicles (with different current state).
2) User should be able to search vehicles by availibilty.
3) User should be able to book vehicles from a specific time period (if available), the vehicle will be blocked for that time duration.
4) Payment will be handled by 3rd party APIs.
5) Concurrent Users are allowed.

Entities:

1) Vehicle
    - id
    - type (Car, Bike, Scooter etc.)
2) User 
    - id
    - metadata etc.
    - list of Vehicles ? (No, idts)

3) Reservation
    - id
    - vehicle id/s associated to it
    - user id
    - Date range
    - bill id / for payment

4) Payment Manager
    - Payment Processor - for Different ways to pay
    + make_payment(reservation) -> Status + id of payment made

5) Reservation Manager
    - reservations : {reservation id : reservation object} ? (how are they different)
    - 