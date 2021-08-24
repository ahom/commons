import { randomBytes } from 'crypto';
import { v1 } from 'uuid';

// Homegrown uuid which is time sorted - a la UUIDv6
export function uuid(): string {
    const raw = v1();
    const prefix = `${raw.substring(15, 18)}${raw.substring(9, 13)}${raw.substring(0, 5)}6${raw.substring(5, 8)}`;
    const chars = randomBytes(8).toString('hex');
    return `${prefix.substr(0, 8)}-${prefix.substr(8, 4)}-${prefix.substr(12)}-${chars.substring(0, 4)}-${chars.substring(4)}`;
}

export function safe<T>(value?: T): T {
    if (value === undefined || value === null) {
        throw new Error('Unsafe');
    } else {
        return value;
    }
}

export function normalizeString(value: string): string {
    return value.normalize('NFD').replace(/[^A-Za-z0-9]/g, '').toLowerCase();
}