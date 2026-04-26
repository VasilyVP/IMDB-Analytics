import { clsx, type ClassValue } from "clsx"
import { twMerge } from "tailwind-merge"
import { ApiError } from "@/lib/exceptions"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

type QueryParamValue = string | number | boolean | null | undefined

type GetFetcherOptions = {
  params?: Record<string, QueryParamValue>
}

type JsonRequestOptions<TBody> = GetFetcherOptions & {
  body?: TBody
  method?: "GET" | "POST" | "PUT" | "PATCH" | "DELETE"
}

function buildApiUrl(path: string, options?: GetFetcherOptions): string {
  const normalizedPath = path.startsWith("/") ? path : `/${path}`
  const url = new URL(`/api${normalizedPath}`, window.location.origin)

  if (options?.params) {
    for (const [key, value] of Object.entries(options.params)) {
      if (value === null || value === undefined) {
        continue
      }

      url.searchParams.set(key, String(value))
    }
  }

  return `${url.pathname}${url.search}`
}

async function requestJson<TResponse, TBody>(path: string, options?: JsonRequestOptions<TBody>): Promise<TResponse> {
  const response = await fetch(buildApiUrl(path, options), {
    method: options?.method ?? "GET",
    headers: options?.body === undefined ? undefined : {
      "Content-Type": "application/json",
    },
    body: options?.body === undefined ? undefined : JSON.stringify(options.body),
  })

  if (!response.ok) {
    throw new ApiError(response.status)
  }

  return (await response.json()) as TResponse
}

export function getFetcher<TResponse>(
  path: string,
  options?: GetFetcherOptions,
): () => Promise<TResponse> {
  return async (): Promise<TResponse> => {
    return requestJson<TResponse, never>(path, options)
  }
}

export function postJson<TResponse, TBody>(path: string, body: TBody): Promise<TResponse> {
  return requestJson<TResponse, TBody>(path, { method: "POST", body })
}
