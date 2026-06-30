// ==========================================
// 🏷️ TYPE DEFINITIONS (Our Safety Guards)
// ==========================================

/**
 * Settings required to open up a connection to the ZynSearch Cluster.
 */
export interface ZynClientOptions {
  /** The full URL of your ZynSearch instance (e.g., 'http://localhost:8000' or 'https://api.zyn.internal') */
  endpoint: string;
  
  /** Optional credentials if Basic Authentication is enabled on your Axum server */
  auth?: {
    username: string;
    password: string;
  };
  
  /** Network limit in milliseconds before dropping a slow request. Defaults to 5000ms. */
  timeoutMs?: number;
}

export interface IndexRequest {
  sourceId: string;
  content: string;
  sourceKind?: 'opaque' | 'filesystem' | 's3object';
  replaceExisting?: boolean;
}

export interface IndexResponse {
  documentId: number;
  sourceId: string;
  status: 'created' | 'replaced' | 'skipped';
}

export interface SearchRequest {
  query: string;
  limit?: number;
  explain?: boolean;
}

export interface SearchResponse {
  query: string;
  results: Array<{
    documentId: number;
    sourceId: string;
    title: string;
    score: number;
    explanation?: string;
  }>;
  stats: {
    totalHits: number;
    truncated: boolean;
  };
}

export interface DeleteResponse {
  documentId: number;
  sourceId: string;
  status: 'deleted';
}

// ==========================================
// 🤖 THE CLIENT CLASS (The Walkie-Talkie)
// ==========================================

export class ZynSearchClient {
  private endpoint: string;
  private authHeader?: string;
  private timeoutMs: number;

  constructor(options: ZynClientOptions) {
    // Strip trailing slashes from the endpoint to keep URL stitching clean
    this.endpoint = options.endpoint.replace(/\/+$/, '');
    this.timeoutMs = options.timeoutMs ?? 5000;

    // If they passed a username and password, we compile the "Basic " token immediately.
    if (options.auth) {
      const credentials = `${options.auth.username}:${options.auth.password}`;
      // Buffer.from is standard in Node.js for safely translating strings to Base64
        const encoded = btoa(credentials);
      this.authHeader = `Basic ${encoded}`;
    }
  }

  /**
   * Internal private helper engine to handle fetch calls, timeouts, and structured errors.
   */
  private async request<T>(path: string, method: 'GET' | 'POST' | 'DELETE', body?: unknown): Promise<T> {
    const url = `${this.endpoint}${path}`;
    
    // Setup our custom header checklist matching your Axum middleware demands
    const headers: Record<string, string> = {
      'Accept': 'application/json',
    };

    if (this.authHeader) {
      headers['Authorization'] = this.authHeader;
    }

    if (body) {
      headers['Content-Type'] = 'application/json';
    }

    // High-standard network timeout configuration using AbortController
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.timeoutMs);

    try {
      const response = await fetch(url, {
        method,
        headers,
        body: body ? JSON.stringify(body) : undefined,
        signal: controller.signal,
      });

      clearTimeout(timeoutId);

      // If the Axum server throws an error code, we extract the unified JSON error shape
      if (!response.ok) {
        let errorDetails = response.statusText;
        try {
          const apiError = await response.json() as { error: { code: string; message: string } };
          errorDetails = `[${apiError.error.code}] ${apiError.error.message}`;
        } catch {
          // Fallback if the body isn't structured JSON
        }
        throw new Error(`ZynSearch Error (${response.status}): ${errorDetails}`);
      }

      return await response.json() as T;
    } catch (error: unknown) {
      clearTimeout(timeoutId);
      if (error instanceof Error && error.name === 'AbortError') {
        throw new Error(`ZynSearch Error: Request timed out after ${this.timeoutMs}ms`);
      }
      throw error;
    }
  }

  // ==========================================
  // 🚀 PUBLIC ACTIONS (The Buttons)
  // ==========================================

  /**
   * Pushes a document text payload up into the cluster inverted indices.
   */
  async index(payload: IndexRequest): Promise<IndexResponse> {
    // CamelCase parameters converted to match snake_case fields expected by Axum's IndexRequestBody
    const nativeBody = {
      source_id: payload.sourceId,
      content: payload.content,
      source_kind: payload.sourceKind,
      replace_existing: payload.replaceExisting,
    };

    const rawResponse = await this.request<any>('/index', 'POST', nativeBody);
    
    // Standardizing the response back to idiomatic JavaScript camelCase
    return {
      documentId: Number(rawResponse.document_id),
      sourceId: rawResponse.source_id,
      status: rawResponse.status,
    };
  }

  /**
   * Executes a scatter-gather query across the cluster nodes.
   */
  async search(request: SearchRequest): Promise<SearchResponse> {
    const queryParams = new URLSearchParams({ q: request.query });
    
    if (request.limit !== undefined) {
      queryParams.append('limit', request.limit.toString());
    }
    if (request.explain !== undefined) {
      queryParams.append('explain', request.explain.toString());
    }

    const rawResponse = await this.request<any>(`/search?${queryParams.toString()}`, 'GET');

    return {
      query: rawResponse.query,
      results: rawResponse.results.map((r: any) => ({
        documentId: Number(r.document_id),
        sourceId: r.source_id,
        title: r.title,
        score: r.score,
        explanation: r.explanation,
      })),
      stats: {
        totalHits: rawResponse.stats.total_hits,
        truncated: rawResponse.stats.truncated,
      },
    };
  }

  /**
   * Deletes a specific document from the cluster index by its source ID or numeric ID.
   */
  async delete(id: string | number): Promise<DeleteResponse> {
    const rawResponse = await this.request<any>(`/index/${id}`, 'DELETE');
    return {
      documentId: Number(rawResponse.document_id),
      sourceId: rawResponse.source_id,
      status: rawResponse.status,
    };
  }
}