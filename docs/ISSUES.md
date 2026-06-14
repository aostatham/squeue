# Known Issues and Simplification Follow-ups

This file tracks deliberate simplification deferrals. It is not a product roadmap.

## Public DTO Naming

`ClaimItem` is used in Java REST client, trusted direct Java client, core DTOs, server DTOs, and OpenAPI. `ClaimedItem` is a clearer canonical name, but renaming it now would create release-candidate API churn across Java and wire documentation.

Decision: keep `ClaimItem` for `0.1.0-rc1`; reconsider a compatibility alias or rename before a stable 1.0 API.

## Example Module Layout

The runnable Java examples are separate Maven modules so smoke tests can target producer, REST worker, and direct worker flows independently.

Decision: keep the split for now. Reconsider consolidation only if release packaging becomes too noisy.

## OpenAPI Maintenance

OpenAPI remains checked in as YAML with drift tests against implemented routes and typed schema checks.

Decision: keep checked-in YAML for the release candidate. Reconsider generated OpenAPI only if manual maintenance becomes the main source of drift.
