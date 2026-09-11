# Monte Chat fork: 1.1.0-monte.3

Protocol v8 adds project discovery, normal Hermes consultations and exact command approvals. The gateway continues supporting v3–v7. New Brainstorm consultations use the ordinary Hermes backend with its existing tools, credentials and approval rules. Their purpose and discussion/topic identity are separate from the backend, so previously accepted restricted research retains its original routing.

The discussion context includes a compact repository catalog (names, aliases, descriptions, paths, GitHub slugs and Factory associations). `list_projects` provides local discovery; consultations need no selected project. Small explicit actions retain Brainstorm; implementation requests still switch to Work. `post_discussion_message` uses trusted Discord origin metadata and a bridge delivery receipt.

Findings without a Git revision stamp are labelled unversioned, rather than stale. Returned task update times remain in saved notes, and restored context includes the current time. Revision comparisons still identify changed repository evidence; a missing local stamp does not invalidate a live GitHub or Factory lookup.

Approvals use the installed Hermes API's `request_id` contract. Pending approvals and queued commands survive reconnect; each response identifies the owner, task, run and request. The default is once. Commands are explained at a pause, and spoken consent must follow the prompt. Discord buttons provide the same choices. Legacy clients and uncorrelated approvals retain containment. Session grants retain Hermes's per-run approval scope; permanent grants require explicit consent.

Task document v2 reads v1 records and preserves backend identity. Selected-session execution uses durable runs; consultation sessions remain independent. Existing scheduling rules apply. SDK controls are `respondApproval`, `reportPostResult`, and optional Discord origin on session/discussion selection. See the Monte Chat repository's `docs/natural-hermes.md` for deployment and rollback.

## Previous release design

### Version 1.1.0-monte.2

This release forks upstream HLV v1.1.0 (commit `ce16d93f475ad138a6efb018e4a0417c6c5a501c`). Protocols v3–v6 retain Work behavior. Protocol v7 supports Work/Brainstorm with the OpenAI adapter, durable discussion notes, registered repository evidence, and an isolated read-only Hermes research backend.

New client frames are `session.mode.set` (correlated inspection or switch), `session.context.set` (correlated discussion/session selection), `context.input` (labelled, silent context), and `playback.state` (`active`, `microphoneActive`). A v7 `session.start` can carry `discussionId`; `session.ready` reports `interactionMode`, `discussionId`, and `brainstormSupported`. Confirmations are `session.mode.changed` and `session.context.changed`. Reused control request ids cannot execute the operation twice. Provider tool ids retain the existing dispatch replay ledger.

The provider exposes local `set_conversation_mode`, `select_project`, `update_discussion_notes`, and `consult_hermes` tools. In Brainstorm the gateway enforces the tool subset: mode/context/notes/research, task inspection/stop, and microphone pause. A switch never submits work. An explicit implementation request switches first, then uses the existing Work tool with the saved context. Work accepted earlier continues.

The optional research endpoint must be authenticated loopback, distinct from Work. Task records persist `backend` and research discussion/project/topic metadata; legacy records use Work. Dispatch, polling, streaming, approval containment and stop use the record's original backend. A global task observer archives research results even while the voice client is disconnected. There is one outstanding investigation per discussion, enforced atomically in the scheduler.

`plugins/monte-research` is a standalone Hermes plugin. Its `pre_tool_call` hook enforces a three-tool allowlist, including hidden and discovery calls through the real agent dispatch path. Repository tools reject unregistered roots, path traversal, symlinks, hard links, Git metadata, ignored files and credential files. File sizes, reads, search results, and execution deadlines are bounded. Git runs without shell evaluation, hooks, fsmonitor, or optional index writes. No Hermes core changes are required.

Private voice state is stored alongside task state unless `HERMES_LIVE_VOICE_STATE_FILE` overrides it. `HERMES_LIVE_REPOSITORY_REGISTRY` selects the registry; `HERMES_LIVE_REPOSITORY_ROOTS` is a JSON list of roots (default `/home/alex/Development`). `HERMES_LIVE_RESEARCH_URL` and `HERMES_LIVE_RESEARCH_API_KEY` configure the isolated profile. Without it Brainstorm remains conversational and consultations return an explicit unavailable result.

Checks: `npm run typecheck`, `npm test`, `npm run check:gateway`, and `npm run check:package`. Run `plugins/monte-research/test_integration.py` with the installed Hermes Python environment and `HERMES_RESEARCH_SOURCE` set if Hermes is not at `/home/alex/.hermes/hermes-agent`. Tests use temporary repositories/state and fake providers; live tests are separate.

Deployment, source archive/checksums, rollback and voice acceptance are maintained in the Monte Chat repository's `docs/brainstorm.md` and `docs/brainstorm-validation.md`. The gateway and browser SDK must use the same versioned package. Do not develop by patching installed packages.

The OpenAI adapter waits for effective session update acknowledgement and inserts context through system conversation items without requesting speech. See [OpenAI realtime conversations](https://developers.openai.com/api/docs/guides/realtime-conversations).
