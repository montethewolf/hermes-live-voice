import { mkdtemp, mkdir, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileSync } from 'node:child_process';
import { expect, it } from 'vitest';
import { RepositoryRegistry } from '../src/application/brainstorm/repository-registry.js';
it('discovers descriptions, normalized GitHub remotes and Factory targets without remote credentials', async () => {
  const root = await mkdtemp(join(tmpdir(), 'monte-catalog-'));
  const factory = join(root, 'monte-factory'), app = join(root, 'lng-gg');
  try {
    for (const path of [factory, app]) { await mkdir(path); execFileSync('git', ['init', '-q', path]); }
    execFileSync('git', ['-C', app, 'remote', 'add', 'origin', 'https://secret:credential@github.com/wabansia/lng-gg.git']);
    await writeFile(join(app, 'package.json'), JSON.stringify({ description: 'Campaign session lifecycle' }));
    await mkdir(join(factory, 'config'));
    await writeFile(join(factory, 'config/factory.toml'), `[repo]\ngithub = "wabansia/lng-gg"\npath = "${app}"\n\n[worker]\nconcurrency = 1\n`);
    const registry = new RepositoryRegistry(join(root, 'registry.json'), [root]);
    await registry.load();
    const project = (await registry.resolve('https://github.com/wabansia/lng-gg/issues/74'))[0];
    expect(project.name).toBe('lng-gg'); expect(project.description).toContain('Campaign');
    expect((await registry.resolve('factory'))[0].factory).toEqual({ repository: 'wabansia/lng-gg', path: app, query: `${factory}/bin/factoryq` });
    expect(JSON.stringify(registry.catalog())).not.toMatch(/secret|credential/);
    expect(JSON.stringify(registry.catalog(600)).length).toBeLessThanOrEqual(600);
    await mkdir(join(root, 'journal')); execFileSync('git', ['init', '-q', join(root, 'journal')]);
    await registry.refresh(true); expect((await registry.list()).map(p => p.name)).toContain('journal');
  } finally { await rm(root, { recursive: true, force: true }); }
});
