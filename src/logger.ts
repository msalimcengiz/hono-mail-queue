import { pino } from 'pino';

export const createLogger = (level: string, role: string) => pino({ level, base: { role } });
