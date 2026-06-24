class User:
    def __init__(self, id, name, created_at):
        self.__id = id
        self.__name = name
        self.__created_at = created_at
    
    @property
    def id(self):
        return self.__id
    
    @property
    def account_id(self):
        return self.__account_ids
    
    @property
    def name(self):
        return self.__name
