from threading import Lock

class Vehicle:
    def __init__(self, id, owner_id, plate_no, model_year, vehicle_status):
        self._id = id
        self._owner_id = owner_id
        self._plate_no = plate_no
        self._model_year = model_year
        self._vehicle_status = vehicle_status
        self.lock = Lock()
        
    @property
    def id(self):
        return self._id

    