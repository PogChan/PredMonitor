from __future__ import annotations

from dataclasses import dataclass, field
import threading
import uuid
from typing import Dict, Optional, Tuple

from rapidfuzz import fuzz, utils


@dataclass
class Cluster:
    centroid: str
    markets: Dict[Tuple[str, str], str] = field(default_factory=dict)


class SemanticRegistry:
    def __init__(self, match_threshold: float) -> None:
        self.match_threshold = match_threshold
        self._clusters: Dict[str, Cluster] = {}
        self._market_index: Dict[Tuple[str, str], str] = {}
        self._lock = threading.Lock()

    def ingest_market(
        self,
        platform: str,
        market_key: str,
        label: Optional[str],
        text_blob: Optional[str],
    ) -> Optional[str]:
        if not market_key:
            return None
        normalized = self._build_text(label, text_blob, market_key)
        key = (platform, market_key)
        with self._lock:
            existing = self._market_index.get(key)
            if existing:
                return existing
            best_id, best_score = self._best_match(normalized)
            if best_id and best_score >= self.match_threshold:
                self._clusters[best_id].markets[key] = normalized
                self._market_index[key] = best_id
                return best_id
            cluster_id = str(uuid.uuid4())
            self._clusters[cluster_id] = Cluster(centroid=normalized, markets={key: normalized})
            self._market_index[key] = cluster_id
            return cluster_id

    def cluster_for_market(
        self,
        platform: str,
        market_key: str,
        label: Optional[str],
        text_blob: Optional[str],
    ) -> Optional[str]:
        return self.ingest_market(platform, market_key, label, text_blob)

    def _best_match(self, text: str) -> Tuple[Optional[str], float]:
        best_id: Optional[str] = None
        best_score = 0.0
        for cluster_id, cluster in self._clusters.items():
            score = fuzz.token_set_ratio(
                text,
                cluster.centroid,
                score_cutoff=self.match_threshold,
                processor=utils.default_process,
            )
            if score > best_score:
                best_score = score
                best_id = cluster_id
        return best_id, best_score

    def _build_text(
        self, label: Optional[str], text_blob: Optional[str], fallback: str
    ) -> str:
        label_value = (label or "").strip()
        blob_value = (text_blob or "").strip()
        if blob_value and label_value and label_value not in blob_value:
            return f"{label_value} {blob_value}"
        if blob_value:
            return blob_value
        if label_value:
            return label_value
        return fallback
