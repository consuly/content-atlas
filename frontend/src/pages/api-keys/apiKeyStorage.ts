const STORAGE_KEY = 'atlas-api-keys-local';

type StoredKeyRecord = Record<
  string,
  {
    apiKey: string;
    appName: string;
    storedAt: string;
  }
>;

const readStore = (): StoredKeyRecord => {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) {
      return {};
    }
    return JSON.parse(raw) as StoredKeyRecord;
  } catch {
    return {};
  }
};

const writeStore = (store: StoredKeyRecord) => {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(store));
  } catch {
    // Ignore write failures (e.g. storage disabled)
  }
};

export const saveApiKeySecret = (keyId: string, apiKey: string, appName: string) => {
  const store = readStore();
  store[keyId] = {
    apiKey,
    appName,
    storedAt: new Date().toISOString(),
  };
  writeStore(store);
};

export const getStoredApiKey = (keyId: string): string | null => {
  const store = readStore();
  const record = store[keyId];
  return record ? record.apiKey : null;
};

export const deleteStoredApiKey = (keyId: string) => {
  const store = readStore();
  if (store[keyId]) {
    delete store[keyId];
    writeStore(store);
  }
};
