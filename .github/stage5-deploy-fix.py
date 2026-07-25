#!/usr/bin/env python3
from pathlib import Path

path = Path("docs/space-compute/manifests/phase5-transport-execution-crds.yaml")
docs = path.read_text().split("\n---\n")
if len(docs) != 3:
    raise SystemExit(f"expected 3 CRD documents, got {len(docs)}")

domain_lines = [
    "type: object",
    "required: [name, clusterID, orbitClass]",
    "properties:",
    "  name: {type: string, minLength: 1, maxLength: 63}",
    "  clusterID: {type: string, minLength: 1, maxLength: 253}",
    "  orbitClass: {type: string, minLength: 1, maxLength: 32}",
]
provenance_lines = [
    "type: object",
    "required: [reporterID, source, digest, sequence]",
    "properties:",
    "  reporterID: {type: string, minLength: 1, maxLength: 253}",
    "  source: {type: string, minLength: 1, maxLength: 253}",
    "  digest: {type: string, pattern: '^[0-9a-f]{64}$'}",
    "  signature: {type: string}",
    "  sequence: {type: integer, format: int64, minimum: 1}",
    "  previousDigest: {type: string, pattern: '^(|[0-9a-f]{64})$'}",
]

def expand_alias(text, key, alias, block):
    lines = text.splitlines()
    out = []
    count = 0
    needle = f"{key}: *{alias}"
    for line in lines:
        stripped = line.strip()
        if stripped == needle:
            indent = line[: len(line) - len(line.lstrip())]
            out.append(f"{indent}{key}:")
            out.extend(f"{indent}  {value}" for value in block)
            count += 1
        else:
            out.append(line)
    return "\n".join(out), count

for index in (1, 2):
    for key in ("source", "destination"):
        docs[index], count = expand_alias(docs[index], key, "domain", domain_lines)
        if count != 1:
            raise SystemExit(f"document {index+1}: expected one {key} domain alias, got {count}")

docs[2], count = expand_alias(docs[2], "provenance", "provenance", provenance_lines)
if count != 1:
    raise SystemExit(f"document 3: expected one provenance alias, got {count}")

path.write_text("\n---\n".join(docs) + "\n")
print("phase5 CRD cross-document aliases normalized")
