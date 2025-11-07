// Local storage utilities
import { DBCredentials } from './types';

const CREDENTIALS_KEY = 'pgcompare_credentials';
const THEME_KEY = 'pgcompare_theme';

export function saveCredentials(credentials: Omit<DBCredentials, 'password'>) {
  if (typeof window !== 'undefined') {
    localStorage.setItem(CREDENTIALS_KEY, JSON.stringify(credentials));
  }
}

export function loadCredentials(): Omit<DBCredentials, 'password'> | null {
  if (typeof window !== 'undefined') {
    const stored = localStorage.getItem(CREDENTIALS_KEY);
    return stored ? JSON.parse(stored) : null;
  }
  return null;
}

export function saveTheme(theme: 'light' | 'dark') {
  if (typeof window !== 'undefined') {
    localStorage.setItem(THEME_KEY, theme);
  }
}

export function loadTheme(): 'light' | 'dark' {
  if (typeof window !== 'undefined') {
    const stored = localStorage.getItem(THEME_KEY);
    return (stored as 'light' | 'dark') || 'light';
  }
  return 'light';
}

