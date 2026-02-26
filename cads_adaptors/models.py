import datetime

import pydantic


class Licence(pydantic.BaseModel):
    """Licence information for a collection."""

    title: str | None = None
    revision: str | None = None
    url: str | None = None


class CollectionMetadata(pydantic.BaseModel):
    """Metadata for a collection."""

    model_config = pydantic.ConfigDict(extra="allow", exclude_none=True)

    title: str | None = None
    licences: list[Licence] | None = None
    doi: str | None = None
    citation: str | None = None
    url: str | None = None


class JobMetadata(pydantic.BaseModel):
    """Metadata for a job."""

    process_id: str | None = None
    user_id: str | None = None
    job_id: str | None = None
    status: str | None = None
    message: str | None = None
    created: datetime.datetime | None = None
    started: datetime.datetime | None = None
    finished: datetime.datetime | None = None
    updated: datetime.datetime | None = None
    origin: str | None = None
    traceback: str | None = None
    user_support_url: str | None = None


class ResultsMetadata(pydantic.BaseModel):
    """Metadata for job results."""

    type: str | None = None
    href: str | None = None
    file_checksum: str | None = pydantic.Field(None, alias="file:checksum")
    file_size: int | None = pydantic.Field(None, alias="file:size")
    file_local_path: str | None = pydantic.Field(None, alias="file:local_path")
