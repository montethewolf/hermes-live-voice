import { readdir, realpath, stat, readFile, mkdir, writeFile, rename } from 'node:fs/promises';
import { join, dirname, basename } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createHash } from 'node:crypto';
const exec = promisify(execFile);
export interface Project { id: string; name: string; path: string; aliases?: string[] }
const normalized = (v: string) => v.toLowerCase().replace(/[^a-z0-9]/g, '');
export class RepositoryRegistry {
  private projects?: Project[];
  private loading?: Promise<Project[]>;
  constructor(readonly path: string, readonly roots = ['/home/alex/Development'], readonly python = 'python3') {}
  load(): Promise<Project[]> {
    if (this.projects) return Promise.resolve(this.projects);
    return this.loading ??= this.discover();
  }
  private async discover() {
    let projects: Project[] = [];
    try {
      const saved = JSON.parse(await readFile(this.path, 'utf8'));
      if (saved.version !== 1 || !Array.isArray(saved.projects)) throw new Error('Invalid repository registry');
      projects = saved.projects;
    } catch (err) { if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err; }
    for (const root of this.roots) {
      const entries = await readdir(root, { withFileTypes: true }).catch(() => []);
      const candidates = [root, ...entries.filter(e => e.isDirectory()).map(e => join(root, e.name))];
      for (const candidate of candidates) {
        if (!await stat(join(candidate, '.git')).catch(() => null)) continue;
        const path = await realpath(candidate);
        if (projects.some(p => p.path === path)) continue;
        projects.push({ id: `repo_${createHash('sha256').update(path).digest('hex').slice(0, 16)}`, name: basename(path), path });
      }
    }
    for (const project of projects) {
      if (!/^repo_[a-f0-9]{16}$/.test(project.id) || typeof project.name !== 'string' || !project.path.startsWith('/')) throw new Error('Invalid registered project');
    }
    await mkdir(dirname(this.path), { recursive: true, mode: 0o700 });
    await writeFile(`${this.path}.tmp`, JSON.stringify({ version: 1, projects }), { mode: 0o600 });
    await rename(`${this.path}.tmp`, this.path);
    return this.projects = projects;
  }
  async resolve(name: string) {
    const projects = await this.load();
    const exact = projects.filter(p => p.id === name || [p.name, ...(p.aliases ?? [])].some(n => normalized(n) === normalized(name)));
    return exact.length ? exact : projects.filter(p => normalized(p.name).includes(normalized(name)));
  }
  async brief(project: string): Promise<{ briefing: string; stamp: string }> {
    await this.load();
    const script = fileURLToPath(new URL('../../../plugins/monte-research/repositories.py', import.meta.url));
    const { stdout } = await exec(this.python, [script, 'brief', this.path, project], { timeout: 15000, maxBuffer: 100000 });
    return JSON.parse(stdout);
  }
}
