import { describe, expect, it } from 'vitest';
import { missingVars, render } from '../../src/templates.js';

describe('templates', () => {
  it('escapes HTML in bodies', () => {
    expect(render('<p>{{name}}</p>', { name: '<script>x</script>' }, { escape: true })).toBe(
      '<p>&lt;script&gt;x&lt;/script&gt;</p>',
    );
  });

  it('strips newlines from subjects to prevent header injection', () => {
    expect(render('Hi {{name}}', { name: 'a\r\nBcc: evil@x.com' }, { escape: false })).toBe('Hi a Bcc: evil@x.com');
  });

  it('reports missing required vars', () => {
    expect(missingVars('password_reset', { name: 'A' })).toEqual(['reset_url']);
    expect(missingVars('welcome', { name: 'A' })).toEqual([]);
  });
});
