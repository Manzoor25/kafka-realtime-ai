"""Confluent / Karapace JSON Schema wire-format codec (0x00 + 4B big-endian ID + UTF-8 JSON)."""

from __future__ import annotations

import json
import logging
import struct
import threading
from pathlib import Path
from typing import Any

import httpx
from jsonschema import Draft7Validator, ValidationError

from realtime_ai.kafka.topics import TOPICS_BY_NAME

logger = logging.getLogger(__name__)

MAGIC_BYTE = 0
HEADER_STRUCT = struct.Struct(">bI")  # 1-byte magic (0x00) + 4-byte unsigned big-endian schema ID


class SchemaWireError(ValueError):
    """Raised when wire header or JSON Schema validation fails."""


class SchemaRegistryCodec:
    """Serializes and deserializes Kafka payloads with Karapace / Confluent JSON Schema Registry.

    Supports both HTTP Karapace registry (`registry_url="http://..."`) and deterministic
    in-memory registry (`registry_url="memory://"`) for fast hermetic tests while enforcing
    the exact 5-byte wire header and Draft-07 JSON Schema validation in both modes.
    """

    def __init__(self, registry_url: str, schemas_dir: Path, timeout: float = 10.0) -> None:
        self._registry_url = registry_url.rstrip("/")
        self._schemas_dir = schemas_dir
        self._in_memory = self._registry_url.startswith("memory://")
        self._client = None if self._in_memory else httpx.Client(timeout=timeout)
        self._lock = threading.Lock()
        self._local_schemas_by_topic: dict[str, dict[str, Any]] = {}
        self._validators_by_topic: dict[str, Draft7Validator] = {}
        self._topic_to_schema_id: dict[str, int] = {}
        self._id_to_validator: dict[int, Draft7Validator] = {}
        self._next_mem_id = 1
        self._load_local_schemas()

    def _load_local_schemas(self) -> None:
        for topic_name, spec in TOPICS_BY_NAME.items():
            if not spec.schema_file:
                continue
            path = self._schemas_dir / spec.schema_file
            if not path.exists():
                raise FileNotFoundError(f"Missing schema file for {topic_name}: {path}")
            schema_obj = json.loads(path.read_text(encoding="utf-8"))
            Draft7Validator.check_schema(schema_obj)
            validator = Draft7Validator(schema_obj)
            self._local_schemas_by_topic[topic_name] = schema_obj
            self._validators_by_topic[topic_name] = validator
            if self._in_memory:
                sid = self._next_mem_id
                self._next_mem_id += 1
                self._topic_to_schema_id[topic_name] = sid
                self._id_to_validator[sid] = validator

    def register_all(self) -> dict[str, int]:
        """Registers all local topic JSON Schemas in Karapace under `<topic>-value` subjects."""
        registered: dict[str, int] = {}
        for topic_name in self._local_schemas_by_topic:
            registered[topic_name] = self.ensure_schema_id(topic_name)
        return registered

    def ensure_schema_id(self, topic: str) -> int:
        with self._lock:
            if topic in self._topic_to_schema_id:
                return self._topic_to_schema_id[topic]

        if topic not in self._local_schemas_by_topic:
            raise KeyError(f"No JSON Schema registered locally for topic '{topic}'")

        assert self._client is not None
        subject = f"{topic}-value"
        payload = {
            "schemaType": "JSON",
            "schema": json.dumps(self._local_schemas_by_topic[topic], separators=(",", ":")),
        }
        resp = self._client.post(f"{self._registry_url}/subjects/{subject}/versions", json=payload)
        resp.raise_for_status()
        schema_id = int(resp.json()["id"])

        with self._lock:
            self._topic_to_schema_id[topic] = schema_id
            self._id_to_validator[schema_id] = self._validators_by_topic[topic]
        return schema_id

    def _validator_for_id(self, schema_id: int, fallback_topic: str | None = None) -> Draft7Validator:
        with self._lock:
            if schema_id in self._id_to_validator:
                return self._id_to_validator[schema_id]

        if self._client is not None:
            resp = self._client.get(f"{self._registry_url}/schemas/ids/{schema_id}")
            resp.raise_for_status()
            body = resp.json()
            schema_obj = json.loads(body["schema"])
            validator = Draft7Validator(schema_obj)
            with self._lock:
                self._id_to_validator[schema_id] = validator
            return validator

        if fallback_topic and fallback_topic in self._validators_by_topic:
            return self._validators_by_topic[fallback_topic]
        raise SchemaWireError(f"Unknown schema ID {schema_id}")

    def encode(self, topic: str, payload: dict[str, Any] | None) -> bytes | None:
        """Encodes a dict payload into Schema Registry wire bytes, or returns None for tombstones."""
        if payload is None:
            return None
        validator = self._validators_by_topic.get(topic)
        if validator is None:
            raise SchemaWireError(f"Topic '{topic}' has no configured JSON Schema validator")
        try:
            validator.validate(payload)
        except ValidationError as exc:
            raise SchemaWireError(f"Schema validation failed on produce to {topic}: {exc.message}") from exc

        schema_id = self.ensure_schema_id(topic)
        header = HEADER_STRUCT.pack(MAGIC_BYTE, schema_id)
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        return header + body

    def decode(self, topic: str, raw: bytes | None) -> dict[str, Any] | None:
        """Decodes Schema Registry wire bytes and validates against the schema; None stays None."""
        if raw is None:
            return None
        if len(raw) < HEADER_STRUCT.size:
            raise SchemaWireError(f"Record on {topic} shorter than 5-byte Schema Registry header")
        magic, schema_id = HEADER_STRUCT.unpack_from(raw, 0)
        if magic != MAGIC_BYTE:
            raise SchemaWireError(f"Invalid magic byte {magic!r} on {topic}; expected 0x00")
        try:
            obj = json.loads(raw[HEADER_STRUCT.size :].decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SchemaWireError(f"Malformed UTF-8/JSON body on {topic}: {exc}") from exc

        validator = self._validator_for_id(schema_id, fallback_topic=topic)
        try:
            validator.validate(obj)
        except ValidationError as exc:
            raise SchemaWireError(
                f"Schema validation failed on consume from {topic} (id={schema_id}): {exc.message}"
            ) from exc
        return obj

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
