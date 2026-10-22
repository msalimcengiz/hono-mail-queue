export type MailClass = 'transactional' | 'bulk';

export interface Template {
  class: MailClass;
  subject: string;
  html: string;
  /** Variables that must be present, checked at enqueue time. */
  required: readonly string[];
}

export const templates = {
  password_reset: {
    class: 'transactional',
    subject: 'Reset your password',
    html: '<p>Hi {{name}},</p><p><a href="{{reset_url}}">Reset your password</a>. The link expires in 30 minutes.</p>',
    required: ['name', 'reset_url'],
  },
  welcome: {
    class: 'transactional',
    subject: 'Welcome, {{name}}',
    html: '<p>Hi {{name}}, your account is ready.</p>',
    required: ['name'],
  },
  campaign_report: {
    class: 'bulk',
    subject: 'Weekly report: {{campaign}}',
    html: '<p>{{campaign}} reached {{impressions}} people this week.</p><p><a href="{{unsubscribe_url}}">Unsubscribe</a></p>',
    required: ['campaign', 'impressions', 'unsubscribe_url'],
  },
} as const satisfies Record<string, Template>;

export type TemplateId = keyof typeof templates;

export const templateIds = Object.keys(templates) as [TemplateId, ...TemplateId[]];

const HTML_ESCAPES: Record<string, string> = {
  '&': '&amp;',
  '<': '&lt;',
  '>': '&gt;',
  '"': '&quot;',
  "'": '&#39;',
};

export function escapeHtml(s: string): string {
  return s.replace(/[&<>"']/g, (c) => HTML_ESCAPES[c] ?? c);
}

/** Replaces {{var}} placeholders. HTML output is escaped; subjects are plain text. */
export function render(source: string, vars: Record<string, string>, opts: { escape: boolean }): string {
  return source.replace(/\{\{\s*(\w+)\s*\}\}/g, (_, key: string) => {
    const v = vars[key] ?? '';
    return opts.escape ? escapeHtml(v) : v.replace(/[\r\n]+/g, ' ');
  });
}

export function missingVars(id: TemplateId, vars: Record<string, string>): string[] {
  return templates[id].required.filter((k) => !vars[k]);
}
