# voice-ci

**Your voice agent changed. Find out from a test, not from a customer.**

voice-ci phones your voice agent with AI callers, then grades every call two ways:

- **Did it do the job?** Each call must leave exactly the record you expect (a lead, a message, a booking). Graded by comparing the final database, so no AI judge decides pass or fail.
- **Was it a good call?** Response speed, turn taking and conciseness, measured from the audio.

Any wrong outcome fails the run, so it works as a gate before you deploy a prompt change.

Built on [ServiceNow/eva](https://github.com/ServiceNow/eva) (MIT), the end-to-end voice agent benchmark. voice-ci adds what you need to use EVA on your own agent, every day:

- **Your agent as deployed.** An adapter for the OpenAI Live API (`gpt-live-1`, with or without a delegated backend model) that replays your app's call-ending logic: closing cues, hang-up after the goodbye, the nudge when the agent says goodbye with nothing recorded.
- **Suites in YAML.** Callers, their goals, what they will say when asked, and the record each call should leave.
- **Your own outcome code.** A small command turns the agent's tool call into the record, so the test runs the same rules as production.
- **A scorecard with an exit code.** PASS/FAIL per call, timing columns, `scorecard.json` per run.

## Quick start

```
bin/voice-ci setup                       # EVA at the pinned commit + patches + deps (needs uv, git)
export OPENAI_API_KEY=...                # caller, agent and judges
export GEMINI_API_KEY=...                # one judge
bin/voice-ci run examples/home-services # the sample agent: 4 calls, about 3 minutes
bin/voice-ci run path/to/agent C1 C4     # some callers of your own agent
bin/voice-ci score path/to/agent         # rescore the newest run
```

## The sample agent

`examples/home-services` is Mike, the phone agent for Brightwater Plumbing, a fictional one-van plumber. Everything in it is synthetic. Four AI callers phone him:

| Caller | Situation | The record the call must leave |
|---|---|---|
| J1 | Wants a quote on a new water heater | customer, job, not urgent, last name, house number |
| J2 | Burst pipe, water through the ceiling | customer, job, **urgent** |
| M1 | Existing customer with a billing question | message, no job |
| S1 | Ad salesman pitching the owner | spam, nothing taken down |

A real run:

```
call  outcome  turns  speed_s  concise  ended
J1    PASS     1.00   1.80     0.79     goodbye
J2    PASS     0.89   1.43     0.88     assistant_disconnect
M1    PASS     1.00   1.37     0.88     assistant_disconnect
S1    PASS     0.83   1.26     1.00     goodbye

4/4 outcomes right
```

The first draft of this agent failed S1: told to "end the call" on salesmen, it said goodbye without recording anything, so the call left no record. That is the class of bug voice-ci exists for: the call sounds fine and the data is wrong. One prompt line fixed it, and the run above is the fixed agent.

## Your own agent

An agent folder holds:

| File | What |
|---|---|
| `agent.yaml` | domain id, framework (`openai_live`), optional `prepare` command, `outcome_command`, `greeting_text` |
| `live.json` | the Live session your app starts, plus call-ending settings (see `voice_ci/adapters/openai_live_server.py`) |
| `suite.yaml` | the callers and their expected records |
| outcome command | reads `{"tool", "args"}` on stdin, prints `{"kind", "record"}` |

## Fixes found running EVA on a production agent

- The Live API streams digital silence between sentences. EVA's caller heard that as the agent still talking, so calls ran to the time limit. Only voiced audio is forwarded.
- The Live API rejects audio faster than real time, and context injection fails unless audio is flowing. Caller audio is paced in real time with line silence in the gaps.
- An agent that hangs up after its goodbye counted as a broken call. A logged hang-up now counts as a valid end.

`patches/eva-core.patch` is small: any domain name, the Live adapter registration, the hang-up rule, a per-agent greeting, and prompt files that merge into a section.

## Status

Early, and used daily on a production phone agent. Next: repeat runs (pass^k), cost per run, a baseline diff and a GitHub Action.

MIT. EVA is MIT, copyright ServiceNow.
