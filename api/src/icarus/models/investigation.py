from pydantic import BaseModel


class InvestigationCreate(BaseModel):
    title: str
    description: str | None = None


class InvestigationUpdate(BaseModel):
    title: str | None = None
    description: str | None = None


class InvestigationResponse(BaseModel):
    id: str
    title: str
    description: str | None = None
    created_at: str
    updated_at: str
    entity_ids: list[str] = []
    share_token: str | None = None


class InvestigationListResponse(BaseModel):
    investigations: list[InvestigationResponse]
    total: int


class Annotation(BaseModel):
    id: str
    entity_id: str
    investigation_id: str
    text: str
    created_at: str


class AnnotationCreate(BaseModel):
    entity_id: str
    text: str


class Tag(BaseModel):
    id: str
    investigation_id: str
    name: str
    color: str


class TagCreate(BaseModel):
    name: str
    color: str = "#E07A2F"
