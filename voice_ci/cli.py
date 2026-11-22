"""voice-ci commands (run through bin/voice-ci, inside EVA's environment)."""

import json
import os
import subprocess
import sys
from pathlib import Path

import yaml

HERE = Path(__file__).parent
sys.path.insert(0, str(HERE))
import build  # noqa: E402
import score  # noqa: E402

# EVA settings that worked on the first agent; any of them can be overridden in agent.yaml (eva_env).
DEFAULT_ENV = {
    "EVA_MODEL__S2S": "openai",
    "JUDGE_MODEL": "gpt-5.2",
    "EVA_USER_SIMULATOR__PROVIDER": "openai_realtime",
    "EVA_USER_SIMULATOR__MODEL": "gpt-realtime-1.5",
    "EVA_USER_SIMULATOR__FEMALE_VOICE": "marin",
    "EVA_USER_SIMULATOR__MALE_VOICE": "cedar",
    "EVA_LANGUAGE": "en",
    "EVA_MAX_CONCURRENT_CONVERSATIONS": "2",
    "EVA_MAX_CONCURRENT_JUDGES": "3",
    "EVA_CONVERSATION_TIME_LIMIT_SECONDS": "300",
    "EVA_MAX_RERUN_ATTEMPTS": "1",
    "EVA_PREFLIGHT": "false",
    "EVA_MODEL_LIST": json.dumps([
        {"model_name": "gpt-5.2", "litellm_params": {"model": "openai/gpt-5.2", "api_key": "os.environ/OPENAI_API_KEY"}, "model_info": {"base_model": "gpt-5.2"}},
        {"model_name": "gemini-3-flash-preview", "litellm_params": {"model": "gemini/gemini-3-flash-preview", "api_key": "os.environ/GEMINI_API_KEY"}},
    ]),
}


def _profile(agent_dir: Path) -> dict:
    return yaml.safe_load((agent_dir / "agent.yaml").read_text())


def _newest_run(agent_dir: Path) -> Path:
    runs = agent_dir / "runs"
    return max((p for p in runs.iterdir() if p.is_dir()), key=lambda p: p.stat().st_mtime)


def cmd_build(eva: Path, agent_dir: Path) -> dict:
    prof = _profile(agent_dir)
    if prof.get("prepare"):
        subprocess.run(prof["prepare"], shell=True, cwd=agent_dir, check=True)
    build.main(agent_dir, eva)
    return prof


def cmd_run(eva: Path, agent_dir: Path, ids: list[str]) -> int:
    if not os.environ.get("OPENAI_API_KEY"):
        sys.exit("voice-ci: OPENAI_API_KEY is not set")
    prof = cmd_build(eva, agent_dir)
    live_path = (agent_dir / prof.get("live_config", "live.json")).resolve()
    live = json.loads(live_path.read_text())
    env = {**os.environ, **DEFAULT_ENV, **{k: str(v) for k, v in (prof.get("eva_env") or {}).items()}}
    env.update({
        "EVA_DOMAIN": prof["domain"],
        "EVA_FRAMEWORK": prof.get("framework", "openai_live"),
        "EVA_OUTPUT_DIR": str((agent_dir / "runs").resolve()),
        "EVA_INITIAL_MESSAGE": live.get("greeting_text") or prof.get("greeting_text", ""),
        "VOICE_CI_AGENT_DIR": str(agent_dir),
        "VOICE_CI_OUTCOME_CMD": prof["outcome_command"],
        "EVA_MODEL__S2S_PARAMS": json.dumps({"model": live["session"]["model"], "api_key": os.environ["OPENAI_API_KEY"], "live_config": str(live_path)}),
    })
    if not env["EVA_INITIAL_MESSAGE"]:
        env.pop("EVA_INITIAL_MESSAGE")
    (agent_dir / "runs").mkdir(exist_ok=True)
    args = ["eva"] + (["--record-ids", ",".join(ids)] if ids else [])
    subprocess.run(args, cwd=eva, env=env, check=False)
    return cmd_score(eva, agent_dir, None)


def cmd_score(eva: Path, agent_dir: Path, run: str | None) -> int:
    prof = _profile(agent_dir)
    run_dir = Path(run).resolve() if run else _newest_run(agent_dir)
    return score.main(run_dir, eva / f"data/{prof['domain']}_dataset.json")


def main() -> None:
    cmd, eva, agent_dir, *rest = sys.argv[1:]
    eva, agent_dir = Path(eva).resolve(), Path(agent_dir).resolve()
    if cmd == "build":
        cmd_build(eva, agent_dir)
    elif cmd == "run":
        sys.exit(cmd_run(eva, agent_dir, rest))
    elif cmd == "score":
        sys.exit(cmd_score(eva, agent_dir, rest[0] if rest else None))


if __name__ == "__main__":
    main()
