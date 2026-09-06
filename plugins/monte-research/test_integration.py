"""Runs against installed Hermes discovery and dispatch, with temporary private state."""
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest

PROFILE = tempfile.TemporaryDirectory(prefix='monte-research-test-')
ROOT = Path(PROFILE.name)
os.environ['HERMES_HOME'] = str(ROOT / 'profile')
os.environ['HERMES_LIVE_REPOSITORY_REGISTRY'] = str(ROOT / 'registry.json')
os.environ['HERMES_SKIP_OPTIONAL_IMPORTS'] = '1'
sys.path.insert(0, os.environ.get('HERMES_RESEARCH_SOURCE', '/home/alex/.hermes/hermes-agent'))
profile = Path(os.environ['HERMES_HOME'])
profile.mkdir(mode=0o700)
shutil.copytree(Path(__file__).parent, profile / 'plugins' / 'monte-research', ignore=shutil.ignore_patterns('__pycache__'))
(profile / 'config.yaml').write_text('plugins:\n  enabled: [monte-research]\n  disabled: []\ntoolsets: [monte_research]\nplatform_toolsets:\n  api_server: [monte_research]\n')
repo = ROOT / 'project'
repo.mkdir()
subprocess.run(['git', 'init', '-q', str(repo)], check=True)
(repo / '.gitignore').write_text('ignored.txt\n')
(repo / 'main.py').write_text('def design():\n    return "option B"\n')
(repo / 'ignored.txt').write_text('PRIVATE ignored')
(repo / '.env').write_text('PRIVATE=credential')
(repo / 'auth.json').write_text('{"PRIVATE": "credential"}')
(repo / 'key.pem').write_text('PRIVATE key')
outside = ROOT / 'private.txt'
outside.write_text('PRIVATE outside')
(repo / 'escape.txt').symlink_to(outside)
(repo / 'inside.py').symlink_to(repo / 'main.py')
(repo / 'dir').symlink_to(ROOT, target_is_directory=True)
# Ignored tracked files must also be excluded.
subprocess.run(['git', '-C', str(repo), 'add', '-f', '.'], check=True)
(ROOT / 'registry.json').write_text(json.dumps({'version': 1, 'projects': [{'id': 'repo_0123456789abcdef', 'name': 'project', 'path': str(repo)}]}))
from hermes_cli.plugins import discover_plugins, invoke_hook
from model_tools import handle_function_call
from tools.registry import registry
from types import SimpleNamespace
from agent.agent_runtime_helpers import invoke_tool

discover_plugins(force=True)
PROJECT = 'repo_0123456789abcdef'

def dispatch(name, args):
    return handle_function_call(name, args, task_id='research-test', session_id='isolated-research', enabled_toolsets=['monte_research'], enabled_tools=['repo_list', 'repo_read', 'repo_search'])

class ResearchIntegration(unittest.TestCase):
    def test_plugin_loaded_and_real_dispatch_reads(self):
        value = dispatch('repo_read', {'project': PROJECT, 'path': 'main.py'})
        self.assertIn('option B', value)
        self.assertNotIn('error', json.loads(value))

    def test_list_search_are_bounded_and_exclude_private_files(self):
        value = dispatch('repo_list', {'project': PROJECT})
        self.assertIn('main.py', value)
        for name in ['.env', '.git/', 'ignored.txt', 'auth.json', 'key.pem', 'escape.txt', 'inside.py']:
            self.assertNotIn(name, value)
        value = dispatch('repo_search', {'project': PROJECT, 'query': 'PRIVATE'})
        self.assertEqual(json.loads(value)['matches'], [])
        self.assertIn('main.py', dispatch('repo_search', {'project': PROJECT, 'query': 'option B'}))

    def test_forbidden_reads_traversal_and_symlinks(self):
        for name in ['../private.txt', str(outside), '.git/config', '.env', 'auth.json', 'ignored.txt', 'escape.txt', 'inside.py', 'dir/private.txt', 'key.pem']:
            with self.subTest(path=name):
                value = dispatch('repo_read', {'project': PROJECT, 'path': name})
                self.assertIn('error', json.loads(value))
                self.assertNotIn('PRIVATE', value)
        self.assertIn('error', dispatch('repo_read', {'project': 'unregistered', 'path': 'main.py'}))

    def test_dispatch_blocks_hidden_tools_even_without_discovery_filter(self):
        sentinel = ROOT / 'must-not-exist'
        def hidden(args, **kwargs):
            sentinel.write_text('bypassed')
            return '{}'
        registry.register(name='hidden_write_probe', toolset='hidden', schema={'name': 'hidden_write_probe', 'parameters': {'type': 'object'}}, handler=hidden)
        for name, args in [('terminal', {'command': 'touch ' + str(sentinel)}), ('write_file', {'path': str(sentinel), 'content': 'bad'}),
                           ('delegate_task', {'task': 'write'}), ('send_message', {'message': 'no'}), ('tool_search', {'query': 'terminal'}),
                           ('execute_code', {'code': 'open("' + str(sentinel) + '", "w")'}), ('hidden_write_probe', {})]:
            with self.subTest(tool=name):
                value = invoke_tool(SimpleNamespace(session_id='isolated-research'), name, args, 'research-test')
                self.assertIn('Research profile permits only', value)
                if name not in ('delegate_task', 'tool_search'):
                    self.assertIn('Research profile permits only', dispatch(name, args))
                self.assertFalse(sentinel.exists())
        self.assertTrue(any(v and v.get('action') == 'block' for v in invoke_hook('pre_tool_call', tool_name='terminal', args={})))

if __name__ == '__main__':
    try:
        unittest.main()
    finally:
        PROFILE.cleanup()
