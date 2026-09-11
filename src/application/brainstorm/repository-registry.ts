import { readdir, realpath, stat, readFile, mkdir, writeFile, rename } from 'node:fs/promises';
import { join, dirname, basename } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { createHash } from 'node:crypto';
const exec = promisify(execFile);
export interface Project {
  id: string; name: string; path: string; aliases?: string[]; description?: string;
  repositories?: string[]; factory?: { repository: string; path?: string; query: string };
  overrides?: { description?: string; aliases?: string[]; repositories?: string[] };
}
const normalized = (v: string) => v.toLowerCase().replace(/[^a-z0-9]/g, '');
const read = (path: string) => readFile(path, 'utf8').catch(() => '');
export class RepositoryRegistry {
  private projects?: Project[];
  private loading?: Promise<Project[]>;
  private refreshedAt = 0;
  constructor(readonly path: string, readonly roots = ['/home/alex/Development'], readonly python = 'python3') {}
  load(): Promise<Project[]> {
    if (this.projects) return Promise.resolve(this.projects);
    return this.loading ??= this.discover().finally(() => { this.loading = undefined; });
  }
  refresh(force = false): Promise<Project[]> {
    if (this.loading) return this.loading;
    if (!force && this.projects && Date.now() - this.refreshedAt < 300000) return Promise.resolve(this.projects);
    return this.loading = this.discover().finally(() => { this.loading = undefined; });
  }
  private async discover() {
    let projects: Project[] = this.projects ?? [];
    if (!this.projects) {
      try {
        const saved = JSON.parse(await readFile(this.path, 'utf8'));
        if (![1, 2].includes(saved.version) || !Array.isArray(saved.projects)) throw new Error('Invalid repository registry');
        projects = saved.projects;
        this.projects = projects; // cached metadata is usable during refresh
      } catch (err) { if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err; }
    }
    projects = structuredClone(projects);
    for (const root of this.roots) {
      const entries = await readdir(root, { withFileTypes: true }).catch(() => []);
      for (const candidate of [root, ...entries.filter(e => e.isDirectory()).map(e => join(root, e.name))]) {
        if (!await stat(join(candidate, '.git')).catch(() => null)) continue;
        const path = await realpath(candidate);
        if (!projects.some(p => p.path === path)) projects.push({ id: `repo_${createHash('sha256').update(path).digest('hex').slice(0, 16)}`, name: basename(path), path });
      }
    }
    for (const project of projects) {
      if (!/^repo_[a-f0-9]{16}$/.test(project.id) || typeof project.name !== 'string' || !project.path.startsWith('/')) throw new Error('Invalid registered project');
      await this.describe(project);
    }
    await mkdir(dirname(this.path), { recursive: true, mode: 0o700 });
    // Retain v1 file format for the legacy research plugin. Metadata is additive.
    await writeFile(`${this.path}.tmp`, JSON.stringify({ version: 1, projects }), { mode: 0o600 });
    await rename(`${this.path}.tmp`, this.path);
    this.refreshedAt = Date.now();
    return this.projects = projects;
  }
  private async describe(project: Project) {
    const [pkg, readme, factory, remotes] = await Promise.all([
      read(join(project.path, 'package.json')), read(join(project.path, 'README.md')),
      read(join(project.path, 'config/factory.toml')),
      exec('git', ['-C', project.path, 'remote', '-v'], { timeout: 3000, maxBuffer: 32000 }).then(r => r.stdout).catch(() => ''),
    ]);
    let description = '';
    try { description = JSON.parse(pkg).description ?? ''; } catch { /* README fallback */ }
    if (!description) description = readme.split(/\n\s*\n/).find(p => /^[A-Za-z]/.test(p.trim()) && !p.includes('```')) ?? '';
    project.description = (project.overrides?.description ?? description).replace(/\s+/g, ' ').slice(0, 320);
    // Only normalized GitHub slugs enter model context; remote credentials never do.
    project.repositories = project.overrides?.repositories ?? [...new Set([...remotes.matchAll(/github\.com[:/]([\w.-]+\/[\w.-]+)/g)].map(m => m[1].replace(/\.git$/, '')))];
    if (project.overrides?.aliases) project.aliases = project.overrides.aliases;
    const section = factory.match(/^\[repo\]\s*\n([\s\S]*?)(?=^\[|$(?![\s\S]))/m)?.[1] ?? '';
    const repository = section.match(/^github\s*=\s*"([\w.-]+\/[\w.-]+)"/m)?.[1];
    if (repository) project.factory = { repository, path: section.match(/^path\s*=\s*"([^"]+)"/m)?.[1], query: `${project.path}/bin/factoryq` };
    else delete project.factory;
  }
  async resolve(name: string) {
    const projects = await this.load();
    const key = normalized(name.replace(/^https?:\/\/github.com\//, '').replace(/\/(issues|pull)\/.*/, ''));
    if (!key) return [];
    const exact = projects.filter(p => p.id === name || [p.name, ...(p.aliases ?? []), ...(p.repositories ?? [])].some(n => normalized(n) === key));
    return exact.length ? exact : projects.filter(p => [p.name, ...(p.aliases ?? [])].some(n => normalized(n).includes(key)));
  }
  async list(query = '') {
    const projects = await this.load();
    return projects.filter(p => !query || normalized(JSON.stringify(p)).includes(normalized(query)));
  }
  catalog(maxChars = 8000) {
    const projects: Project[] = [];
    for (const p of this.projects ?? []) {
      const entry = { id: p.id, name: p.name, path: p.path, description: p.description, aliases: p.aliases, repositories: p.repositories, factory: p.factory };
      if (JSON.stringify([...projects, entry]).length > maxChars - 300) break;
      projects.push(entry);
    }
    return { projects, truncated: projects.length < (this.projects?.length ?? 0), discovery: 'Use list_projects for other matches; consult_hermes can investigate without a selected project.' };
  }
  async settled() { await this.loading; }
  async brief(project: string): Promise<{ briefing: string; stamp: string }> {
    await this.load();
    const script = fileURLToPath(new URL('../../../plugins/monte-research/repositories.py', import.meta.url));
    const { stdout } = await exec(this.python, [script, 'brief', this.path, project], { timeout: 15000, maxBuffer: 100000 });
    return JSON.parse(stdout);
  }
}
