"""One error shape for the whole API:

    { "error": { "code": "INSUFFICIENT_BALANCE", "message": "...", "details": [ ... ] } }
"""


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: list | None = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or []

    def body(self) -> dict:
        err: dict = {"code": self.code, "message": self.message}
        if self.details:
            err["details"] = self.details
        return {"error": err}
