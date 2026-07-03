export class ApiError extends Error {
  readonly status: number;

  constructor(status: number, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

type RequestOptions = Omit<RequestInit, "body"> & {
  csrf?: string;
  body?: BodyInit | Record<string, unknown> | null;
};

export async function apiRequest<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { csrf = "", body, ...init } = options;
  const headers = new Headers(init.headers);
  const method = (init.method ?? "GET").toUpperCase();
  let requestBody = body as BodyInit | null | undefined;

  if (method !== "GET" && csrf) {
    headers.set("x-csrf-token", csrf);
  }
  if (body && typeof body === "object" && !(body instanceof FormData) && !(body instanceof Blob)) {
    requestBody = JSON.stringify(body);
    headers.set("content-type", "application/json");
  }

  const response = await fetch(path, {
    ...init,
    method,
    headers,
    body: requestBody,
    credentials: "same-origin",
  });

  if (!response.ok) {
    const message = await response.text().catch(() => response.statusText);
    throw new ApiError(response.status, message || response.statusText);
  }

  if (response.status === 204) {
    return undefined as T;
  }

  return (await response.json()) as T;
}
