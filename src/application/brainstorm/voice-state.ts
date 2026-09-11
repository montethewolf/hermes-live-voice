import type { TaskRecord } from '../../domain/tasks/index.js';
import { mkdir, readFile, writeFile, rename, unlink } from 'node:fs/promises';
import { dirname } from 'node:path';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';

export const ModeSchema = z.enum(['work', 'brainstorm']);
export type InteractionMode = z.infer<typeof ModeSchema>;
export const NotesSchema = z.object({
  goals: z.string().default(''), alternatives: z.string().default(''), rejected: z.string().default(''),
  decisions: z.string().default(''), constraints: z.string().default(''), questions: z.string().default(''),
}).strict().refine(n => Object.values(n).join('').length <= 6000, 'Notes exceed 6,000 characters');
const DialogueSchema = z.object({ role: z.enum(['user', 'assistant']), text: z.string().max(20000), interrupted: z.boolean().optional(), itemId: z.string().optional() });
const FindingSchema = z.object({ taskId: z.string(), project: z.string().optional(), generation: z.number(), question: z.string(),
  summary: z.string().max(8000), stamp: z.string().optional(), taskUpdatedAt: z.number().int().nonnegative().optional(), delivered: z.boolean().default(false) });
export const DiscussionSchema = z.object({
  project: z.string().optional(), generation: z.number().int().nonnegative().default(0),
  notes: NotesSchema.default({}), dialogue: z.array(DialogueSchema).max(20).default([]),
  findings: z.array(FindingSchema).max(20).default([]), briefing: z.string().max(12000).default(''),
  evidenceStamp: z.string().optional(), history: z.string().max(20000).default(''),
}).strict();
export type Discussion = z.infer<typeof DiscussionSchema>;
const StateSchema = z.object({ version: z.literal(1), owners: z.record(z.object({ mode: ModeSchema,
  discussions: z.record(DiscussionSchema) }).strict()) }).strict();

/** One instance per gateway. Atomic writes, private permissions, serialized read/modify/write. */
export class VoiceStateStore {
  private value?: z.infer<typeof StateSchema>;
  private tail: Promise<unknown> = Promise.resolve();
  constructor(readonly path: string) {}
  async load() {
    if (this.value) return this.value;
    try { this.value = StateSchema.parse(JSON.parse(await readFile(this.path, 'utf8'))); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw new Error('Voice state unreadable; saved discussions preserved');
      this.value = { version: 1, owners: {} };
    }
    return this.value;
  }
  async retainResearch(record: TaskRecord) {
    if ((record.backend !== 'research' && record.purpose !== 'consultation') || !record.research || !['completed', 'failed', 'cancelled'].includes(record.status)) return;
    const tag = record.research;
    await this.update(record.ownerId, tag.discussionId, d => {
      if (d.findings.some(f => f.taskId === record.taskId)) return;
      d.findings.push({ taskId: record.taskId, project: tag.project, generation: tag.generation, question: tag.question,
        stamp: tag.stamp, taskUpdatedAt: record.updatedAt, summary: (record.output ?? `Investigation ${record.status}; verification unavailable. ${record.error ?? ''}`).slice(0, 8000), delivered: false });
      d.findings = d.findings.slice(-20);
    });
  }
  async get(owner: string, id: string) {
    await this.tail;
    const state = await this.load();
    return { mode: state.owners[owner]?.mode ?? 'work' as InteractionMode,
      discussion: structuredClone(state.owners[owner]?.discussions[id] ?? DiscussionSchema.parse({})) };
  }
  update(owner: string, id: string, mutate: (discussion: Discussion, mode: InteractionMode) => InteractionMode | void) {
    const op = this.tail.then(async () => {
      const next = structuredClone(await this.load());
      const account = next.owners[owner] ??= { mode: 'work', discussions: {} };
      const discussion = account.discussions[id] ??= DiscussionSchema.parse({});
      account.mode = mutate(discussion, account.mode) ?? account.mode;
      StateSchema.parse(next);
      await mkdir(dirname(this.path), { recursive: true, mode: 0o700 });
      const tmp = `${this.path}.${randomUUID()}.tmp`;
      try {
        await writeFile(tmp, JSON.stringify(next), { mode: 0o600, flag: 'wx' });
        await rename(tmp, this.path);
        this.value = next;
      } finally { await unlink(tmp).catch(() => {}); }
      return { mode: account.mode, discussion: structuredClone(discussion) };
    });
    this.tail = op.catch(() => {});
    return op;
  }
}

export function appendDialogue(discussion: Discussion, role: 'user' | 'assistant', text: string, interrupted = false, itemId?: string) {
  if (!text.trim()) return;
  discussion.dialogue.push({ role, text: text.slice(-20000), ...(interrupted ? { interrupted: true } : {}), ...(itemId ? { itemId } : {}) });
  discussion.dialogue = discussion.dialogue.slice(-20);
  while (discussion.dialogue.reduce((n, m) => n + m.text.length, 0) > 20000) discussion.dialogue.shift();
}
