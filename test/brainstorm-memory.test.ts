import { mkdtemp, rm, readFile, stat, writeFile, mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { afterEach, expect, it, vi } from 'vitest';
import { appendDialogue, VoiceStateStore } from '../src/application/brainstorm/voice-state.js';
import { ConversationBrain } from '../src/application/brainstorm/conversation-brain.js';
import { RepositoryRegistry } from '../src/application/brainstorm/repository-registry.js';
import type { TaskSupervisorPort } from '../src/application/live-gateway/ports/task-supervisor.port.js';
import { createTaskRecord, transitionTask } from '../src/domain/tasks/index.js';
const roots: string[] = [];
const brains: ConversationBrain[] = [];
afterEach(async () => { for (const brain of brains.splice(0)) await brain.close(); for (const root of roots.splice(0)) await rm(root, { recursive: true, force: true }); });
async function setup() {
  const root = await mkdtemp(join(tmpdir(), 'monte-brain-')); roots.push(root);
  const store = new VoiceStateStore(join(root, 'private', 'voice.json'));
  const registry = new RepositoryRegistry(join(root, 'repos.json'), []);
  await writeFile(registry.path, JSON.stringify({ version: 1, projects: [{ id: 'repo_0123456789abcdef', name: 'Monte Chat', path: root }] }));
  let idle = true;
  const provider = { updateConfiguration: vi.fn(async (_instructions: string, _tools: readonly string[]) => {}), insertContext: vi.fn(async () => {}), requestContextResponse: vi.fn(async () => {}), close: vi.fn(async () => {}) };
  const tasks = { listActive: vi.fn(async () => []) } as unknown as TaskSupervisorPort;
  const brain = new ConversationBrain({ ownerId: 'owner', sessionKey: 'voice', discussionId: 'discussion_a', store, registry, tasks,
    provider: () => provider as any, workInstruction: () => 'Work', workTools: () => ['start_background_task'], idle: () => idle,
    changed: () => {}, error: () => {}, researchAvailable: false });
  brains.push(brain); await brain.init();
  return { root, store, brain, provider, registry, setIdle: (v: boolean) => { idle = v; } };
}
it('bounds private dialogue and notes, rejects corrupt state, and restores mode on a fresh store', async () => {
  const { store, brain } = await setup();
  await brain.setMode('brainstorm');
  await store.update('owner', 'discussion_a', d => {
    for (let i = 0; i < 30; i++) appendDialogue(d, 'user', `${i}:` + 'a'.repeat(1500));
  });
  const recovered = await new VoiceStateStore(store.path).get('owner', 'discussion_a');
  expect(recovered.mode).toBe('brainstorm');
  expect(recovered.discussion.dialogue.length).toBeLessThanOrEqual(20);
  expect(recovered.discussion.dialogue.reduce((n, m) => n + m.text.length, 0)).toBeLessThanOrEqual(20000);
  expect((await stat(store.path)).mode & 0o777).toBe(0o600);
  await expect(brain.updateNotes({ goals: 'x'.repeat(6001) })).rejects.toThrow();
  await writeFile(store.path, '{bad');
  await expect(new VoiceStateStore(store.path).get('owner', 'discussion_a')).rejects.toThrow('preserved');
  expect(await readFile(store.path, 'utf8')).toBe('{bad');
});
it('requires finalized user acceptance for decisions and labels interrupted assistant replies', async () => {
  const { brain } = await setup();
  brain.text('assistant', 'Option B is probably best.', true); brain.audio('audio_b');
  await brain.finishAssistant();
  await brain.interrupt('audio_b');
  await expect(brain.updateNotes({ decisions: 'Option B', decision_evidence: 'Option B is probably best.' })).rejects.toThrow('exact quote');
  brain.text('user', 'Yes, choose option B.', true);
  await brain.updateNotes({ decisions: 'Option B', decision_evidence: 'Yes, choose option B.' });
  expect(brain.discussion.dialogue[0].interrupted).toBe(true);
  expect(await brain.handoff('Implement B')).toContain('User acceptance');
});
it('rolls provider configuration back on persistence failure and retains the confirmed mode', async () => {
  const { store, brain, provider } = await setup();
  await mkdir(store.path, { recursive: true }); // Prevent atomic rename to simulate a failed state write.
  await expect(brain.setMode('brainstorm')).rejects.toThrow();
  expect(brain.mode).toBe('work');
  expect(provider.updateConfiguration).toHaveBeenCalledTimes(2);
  expect(provider.updateConfiguration.mock.calls.at(-1)?.[0]).toContain('Work');
});
it('retains late findings for superseded topics without announcing or injecting them, and isolates focus', async () => {
  const { brain, provider, store } = await setup();
  await brain.setMode('brainstorm', 'Monte Chat');
  await brain.selectProject('Monte Chat', true);
  const raw = createTaskRecord({ ownerIdentity: 'owner', input: 'Where?', backend: 'research', research: { discussionId: 'discussion_a', project: 'repo_0123456789abcdef', generation: 1, question: 'Where?', stamp: 'old-revision' } });
  const completed = transitionTask(transitionTask(transitionTask(raw, 'dispatching'), 'running', { runId: 'run' }), 'completed', { output: 'Late evidence' });
  await brain.receive(completed); await brain.flush();
  expect(brain.discussion.findings).toHaveLength(1);
  expect(provider.requestContextResponse).not.toHaveBeenCalled();
  expect(brain.context()).not.toContain('Late evidence');
  await brain.updateNotes({ goals: 'Private goal' });
  await brain.switchDiscussion('discord:new');
  expect(brain.mode).toBe('brainstorm'); expect(brain.context()).not.toContain('Private goal');
  expect((await store.get('owner', 'discussion_a')).discussion.notes.goals).toBe('Private goal');
});
it('stays conversational when research is unavailable and avoids an unrestricted fallback', async () => {
  const { brain } = await setup();
  await brain.setMode('brainstorm');
  expect(await brain.consult('Explain architecture')).toMatchObject({ ok: false });
  expect(brain.mode).toBe('brainstorm');
});
it('keeps an accepted Work handoff in its original discussion while context refresh overlaps focus', async () => {
  const { brain, registry } = await setup();
  let finish!: (value: { briefing: string; stamp: string }) => void;
  vi.spyOn(registry, 'brief').mockReturnValue(new Promise(resolve => { finish = resolve; }));
  await brain.updateNotes({ goals: 'Original design' });
  await brain.setMode('work', 'Monte Chat');
  const handoff = brain.handoff('Implement the original design');
  await brain.switchDiscussion('discord:other');
  await brain.updateNotes({ goals: 'Unrelated private discussion' });
  finish({ briefing: 'Verified original repository', stamp: 'new-stamp' });
  const text = await handoff;
  expect(text).toContain('Original design');
  expect(text).toContain('Verified original repository');
  expect(text).toContain('discussion_a');
  expect(text).not.toContain('Unrelated private discussion');
});
