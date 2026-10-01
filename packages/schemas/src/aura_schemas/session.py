from pydantic import BaseModel, Field


class SessionRequest(BaseModel):
    customer_id: str = Field(min_length=1, max_length=128)
    locale: str = Field(default="zh-CN", pattern=r"^[a-z]{2}-[A-Z]{2}$")
    persona: str = Field(default="standard", min_length=1, max_length=64)


class SessionResponse(BaseModel):
    session_id: str
    customer_id: str
    locale: str
    persona: str
