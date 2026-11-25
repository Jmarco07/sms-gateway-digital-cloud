"""Request bodies. Field-level checks that need no database live here."""
from pydantic import BaseModel, ConfigDict, Field, field_validator


class MessageIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    to: str = Field(min_length=1, max_length=32)
    from_: str = Field(alias="from", min_length=1, max_length=16)
    text: str = Field(min_length=1, max_length=2000)
    client_ref: str | None = Field(default=None, max_length=64)

    @field_validator("text")
    @classmethod
    def _no_blank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("must not be blank")
        return value


class BatchIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    messages: list[MessageIn] = Field(min_length=1, max_length=1000)


class WebhookIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str | None = Field(default=None, max_length=500)

    @field_validator("url")
    @classmethod
    def _http_only(cls, value: str | None) -> str | None:
        if value is not None and not value.startswith(("http://", "https://")):
            raise ValueError("must start with http:// or https://")
        return value
