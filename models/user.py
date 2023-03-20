class User:

    def __init__(self, **kwargs):
        self.username: str = ""
        self.uid: str = ""
        self.claims: dict = {}
        self.permissions: list = []
        self.user_details: dict = {}
        self.token: str = ""
        self.__dict__.update((k, v) for k, v in kwargs.items() if k in self.__dict__.keys())

    def __str__(self) -> str:
        text: str = ""
        try:
            for key, value in self.__dict__.items():
                text += str(key) + ":" + str(value) + " "
            return text
        except (KeyError, ValueError) as exc:
            return repr(exc)
