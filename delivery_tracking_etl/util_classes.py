class OperationResult:
    def __init__(self, success: bool, status: str, message: str):
        self.OperationSuccessfull = success
        self.OperationStatus = status
        self.OperationMessage = message

    def __str__(self):
        return f"OperationSuccessfull: {self.OperationSuccessfull}, OperationStatus: {self.OperationStatus}, OperationMessage: {self.OperationMessage}"

class DataOperationResult(OperationResult):
    def __init__(self, success: bool, status: str, message: str, data=None):
        super().__init__(success, status, message)
        self.Data = data

    def __str__(self):
        return f"{super().__str__()}, Data: {self.Data}"