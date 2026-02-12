import logging

from app2.clients.football_api import FootballApiClient
from app2.db.audit import audit_log
from app2.db.connection import get_engine
from app2.db.batch import log_batch_status
from app2.loaders.raw_staging import load_raw
from app2.utils.rate_limit import RateLimiter
from app2.utils.timezone import set_moscow_timezone
from app2.mutators.stg_mutations import mutate_payload

set_moscow_timezone()
logger = logging.getLogger(__name__)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S %z",
)


def run_football_ingestion(
    dag_id: str | None = None,
    run_id: str | None = None,
    *,
    apply_mutations: bool = False,
    run_validations: bool = False,
):
    engine = get_engine()
    client = FootballApiClient()
    limiter = RateLimiter(max_calls=10, window_seconds=60)
    meta = {k: v for k, v in {"dag_id": dag_id, "run_id": run_id}.items() if v is not None}
    logger.info("Start football ingestion: dag_id=%s run_id=%s", dag_id, run_id)
    rows_processed = 0
    validations_payloads: dict[str, object] | None = {} if run_validations else None

    if dag_id and run_id:
        audit_log(engine, dag_id=dag_id, run_id=run_id, layer="STG", entity_name="raw_football_api", status="STARTED")
        log_batch_status(engine, dag_id=dag_id, run_id=run_id, parent_run_id=run_id, layer="STG", status="NEW")
        log_batch_status(engine, dag_id=dag_id, run_id=run_id, parent_run_id=run_id, layer="STG", status="PROCESSING")

    try:
        def _is_processable(kind: str, status_code: int, payload) -> tuple[bool, str]:
            try:
                code = int(status_code or 0)
            except Exception:
                code = 0
            if code < 200 or code >= 300:
                return False, f"http_status={code}"
            if not isinstance(payload, dict):
                return False, "payload is not an object"
            required_key = {
                "competitions": "competitions",
                "areas": "areas",
                "teams": "teams",
                "scorers": "scorers",
                "matches": "matches",
                "standings": "standings",
            }.get(kind)
            if required_key and required_key not in payload:
                return False, f"missing key '{required_key}'"
            return True, "ok"

        def _maybe_load(kind: str, endpoint: str, status_code: int, payload) -> tuple[int, bool, str]:
            ok, reason = _is_processable(kind, status_code, payload)
            if not ok:
                if dag_id and run_id:
                    audit_log(
                        engine,
                        dag_id=dag_id,
                        run_id=run_id,
                        layer="STG",
                        entity_name=f"raw_football_api_skip_{kind}",
                        status="SKIPPED",
                        message=f"{endpoint}: {reason}",
                    )
                return 0, False, reason
            return load_raw(engine, endpoint=endpoint, status_code=status_code, payload=payload, metadata=meta), True, "ok"

        waited = limiter.wait()
        if waited:
            logger.info("Rate limit wait before competitions: %.2fs", waited)
        status_code_comp, payload_comp = client.fetch_competitions()
        logger.info("Fetched competitions: status=%s", status_code_comp)
        if apply_mutations:
            payload_comp, _ = mutate_payload(engine, "STG", dag_id, run_id, "competitions", payload_comp)
        if validations_payloads is not None:
            validations_payloads["competitions_schema"] = payload_comp
            validations_payloads["competitions_completeness"] = payload_comp
            validations_payloads["competitions_uniqueness"] = payload_comp
            validations_payloads["competitions_consistency"] = payload_comp
        rows, ok, _ = _maybe_load("competitions", "competitions", status_code_comp, payload_comp)
        rows_processed += rows

        waited = limiter.wait()
        if waited:
            logger.info("Rate limit wait before areas: %.2fs", waited)
        status_code_areas, payload_areas = client.fetch_areas()
        logger.info("Fetched areas: status=%s", status_code_areas)
        if apply_mutations:
            payload_areas, _ = mutate_payload(engine, "STG", dag_id, run_id, "areas", payload_areas)
        if validations_payloads is not None:
            validations_payloads["areas_schema"] = payload_areas
            validations_payloads["areas_completeness"] = payload_areas
            validations_payloads["areas_uniqueness"] = payload_areas
        rows, ok, _ = _maybe_load("areas", "areas", status_code_areas, payload_areas)
        rows_processed += rows

        competition_ids = []
        if payload_comp and isinstance(payload_comp, dict):
            competition_ids = [
                comp.get("id")
                for comp in payload_comp.get("competitions", [])
                if isinstance(comp, dict) and isinstance(comp.get("id"), int)
            ]
        logger.info("Competitions fetched: %s", competition_ids)

        seasons = [2023, 2024, 2025]

        for competition_id in competition_ids:
            for season in seasons:
                waited = limiter.wait()
                if waited:
                    logger.info("Rate limit wait before teams comp=%s season=%s: %.2fs", competition_id, season, waited)
                status_code_teams, payload_teams = client.fetch_competition_teams(competition_id, season)
                logger.info("Fetched teams comp=%s season=%s status=%s", competition_id, season, status_code_teams)
                if apply_mutations:
                    payload_teams, _ = mutate_payload(engine, "STG", dag_id, run_id, "teams", payload_teams)
                rows, ok, reason = _maybe_load("teams", f"competitions/{competition_id}/teams?season={season}", status_code_teams, payload_teams)
                rows_processed += rows
                if not ok:
                    if reason.startswith("http_status="):
                        logger.info("Skipping competition=%s due to teams endpoint error: %s", competition_id, reason)
                        break
                    continue
                if validations_payloads is not None:
                    validations_payloads["teams_schema"] = payload_teams
                    validations_payloads["teams_completeness"] = payload_teams
                    validations_payloads["teams_uniqueness"] = payload_teams
                    validations_payloads["teams_consistency"] = payload_teams

                waited = limiter.wait()
                if waited:
                    logger.info("Rate limit wait before scorers comp=%s season=%s: %.2fs", competition_id, season, waited)
                status_code_scorers, payload_scorers = client.fetch_competition_scorers(competition_id, season, limit=50)
                logger.info("Fetched scorers comp=%s season=%s status=%s", competition_id, season, status_code_scorers)
                if apply_mutations:
                    payload_scorers, _ = mutate_payload(engine, "STG", dag_id, run_id, "scorers", payload_scorers)
                rows, ok, reason = _maybe_load("scorers", f"competitions/{competition_id}/scorers?season={season}&limit=50", status_code_scorers, payload_scorers)
                rows_processed += rows
                if not ok:
                    if reason.startswith("http_status="):
                        logger.info("Skipping competition=%s due to scorers endpoint error: %s", competition_id, reason)
                        break
                    continue
                if validations_payloads is not None:
                    validations_payloads["scorers_schema"] = payload_scorers
                    validations_payloads["scorers_completeness"] = payload_scorers
                    validations_payloads["scorers_uniqueness"] = payload_scorers
                    validations_payloads["scorers_consistency"] = payload_scorers

                waited = limiter.wait()
                if waited:
                    logger.info("Rate limit wait before matches comp=%s season=%s: %.2fs", competition_id, season, waited)
                status_code_matches, payload_matches = client.fetch_competition_matches(competition_id, season)
                logger.info("Fetched matches comp=%s season=%s status=%s", competition_id, season, status_code_matches)
                if apply_mutations:
                    payload_matches, _ = mutate_payload(engine, "STG", dag_id, run_id, "matches", payload_matches)
                rows, ok, reason = _maybe_load("matches", f"competitions/{competition_id}/matches?season={season}", status_code_matches, payload_matches)
                rows_processed += rows
                if not ok:
                    if reason.startswith("http_status="):
                        logger.info("Skipping competition=%s due to matches endpoint error: %s", competition_id, reason)
                        break
                    continue
                if validations_payloads is not None:
                    validations_payloads["matches_schema"] = payload_matches
                    validations_payloads["matches_completeness"] = payload_matches
                    validations_payloads["matches_uniqueness"] = payload_matches
                    validations_payloads["matches_consistency"] = payload_matches

                waited = limiter.wait()
                if waited:
                    logger.info("Rate limit wait before standings comp=%s season=%s: %.2fs", competition_id, season, waited)
                status_code_standings, payload_standings = client.fetch_competition_standings(competition_id, season)
                logger.info("Fetched standings comp=%s season=%s status=%s", competition_id, season, status_code_standings)
                if apply_mutations:
                    payload_standings, _ = mutate_payload(engine, "STG", dag_id, run_id, "standings", payload_standings)
                rows, ok, reason = _maybe_load("standings", f"competitions/{competition_id}/standings?season={season}", status_code_standings, payload_standings)
                rows_processed += rows
                if not ok:
                    if reason.startswith("http_status="):
                        logger.info("Skipping competition=%s due to standings endpoint error: %s", competition_id, reason)
                        break
                    continue
                if validations_payloads is not None:
                    validations_payloads["standings_schema"] = payload_standings
                    validations_payloads["standings_completeness"] = payload_standings
                    validations_payloads["standings_uniqueness"] = payload_standings
                    validations_payloads["standings_consistency"] = payload_standings

        logger.info("Football ingestion completed: dag_id=%s run_id=%s", dag_id, run_id)
        if dag_id and run_id and run_validations:
            from app2.validators.stg.schema_suite import run_stg_schema_suite
            from app2.validators.stg.completeness_suite import run_stg_completeness_suite
            from app2.validators.stg.uniqueness_suite import run_stg_uniqueness_suite
            from app2.validators.stg.consistency_suite import run_stg_consistency_suite
            from app2.validators.stg.ingestion_suite import run_stg_ingestion_suite

            run_stg_ingestion_suite(engine=engine, dag_id=dag_id, run_id=run_id, parent_run_id=run_id)
            run_stg_schema_suite(
                engine=engine,
                dag_id=dag_id,
                run_id=run_id,
                parent_run_id=run_id,
                payloads=validations_payloads or {},
            )
            run_stg_completeness_suite(
                engine=engine,
                dag_id=dag_id,
                run_id=run_id,
                parent_run_id=run_id,
                payloads=validations_payloads or {},
            )
            run_stg_uniqueness_suite(
                engine=engine,
                dag_id=dag_id,
                run_id=run_id,
                parent_run_id=run_id,
                payloads=validations_payloads or {},
            )
            run_stg_consistency_suite(
                engine=engine,
                dag_id=dag_id,
                run_id=run_id,
                parent_run_id=run_id,
                payloads=validations_payloads or {},
            )
        if dag_id and run_id:
            audit_log(engine, dag_id=dag_id, run_id=run_id, layer="STG", entity_name="raw_football_api", status="SUCCESS", rows_processed=rows_processed)
            log_batch_status(engine, dag_id=dag_id, run_id=run_id, parent_run_id=run_id, layer="STG", status="SUCCESS")
    except Exception as e:
        logger.exception("Football ingestion failed: %s", e)
        if dag_id and run_id:
            audit_log(engine, dag_id=dag_id, run_id=run_id, layer="STG", entity_name="raw_football_api", status="FAILED", message=str(e))
            log_batch_status(engine, dag_id=dag_id, run_id=run_id, parent_run_id=run_id, layer="STG", status="FAILED", error_message=str(e))
        raise


if __name__ == "__main__":
    run_football_ingestion()
