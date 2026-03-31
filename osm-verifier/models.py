from pydantic import BaseModel
from typing import List, Optional


class SourceSignal(BaseModel):
    source: str
    status: str                         # "ACTIVE", "CLOSED", "UNKNOWN"
    confidence: float
    last_activity_date: Optional[str] = None
    detail: Optional[str] = None


class NearbyPlace(BaseModel):
    name: str
    osm_id: Optional[str] = None
    lat: float
    lon: float
    distance_m: float
    category: str
    confidence_score: float
    recommendation: str


class PipelineStep(BaseModel):
    id: str
    title: str
    status: str                         # "done", "skipped"
    duration_ms: float
    sources: List[str] = []
    reason: Optional[str] = None


class VerifyRequest(BaseModel):
    name: str
    address: str


class VerifyResponse(BaseModel):
    # Human-readable block (matches sample format)
    summary: str

    # Core
    place_name: str
    address: str
    lat: Optional[float] = None
    lon: Optional[float] = None
    osm_id: Optional[str] = None
    osm_found: bool

    # Verdict
    predicted_status: str               # "Open", "Closed", "Review"
    recommendation: str                 # "ACCEPT", "REVIEW", "REJECT"
    confidence: int                     # 0–100
    narrative: str
    conflict_flag: bool
    contradiction_flag: bool = False
    contradiction_recorded: bool = False

    # Source evidence
    sources: List[SourceSignal] = []
    confirmed_from: List[str] = []
    considered_sources: List[str] = []
    active_sources: List[str] = []
    closure_sources: List[str] = []
    source_count: int = 0
    matched_sources: List[SourceSignal] = []
    matched_source_count: int = 0
    confidence_formula: Optional[str] = None

    # Staleness context
    edit_age_days: Optional[int] = None
    neighbourhood_activity_score: Optional[float] = None
    prior_p_active: Optional[float] = None

    # Mapillary visual
    visual_delta_score: Optional[float] = None
    change_class: Optional[str] = None
    mapillary_before_image_url: Optional[str] = None
    mapillary_after_image_url: Optional[str] = None
    mapillary_before_date: Optional[str] = None
    mapillary_after_date: Optional[str] = None

    # OSM write-back
    changeset_diff: Optional[dict] = None

    # Nearby alternatives
    nearby_places: Optional[List[NearbyPlace]] = None

    # Pipeline trace
    pipeline_steps: List[dict] = []
    osm_edit_url: Optional[str] = None