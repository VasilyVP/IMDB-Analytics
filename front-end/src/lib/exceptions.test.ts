import { describe, expect, it } from "bun:test";

import { ApiError } from "./exceptions";

describe("ApiError", () => {
  it("stores status and keeps default message", () => {
    const error = new ApiError(429);

    expect(error).toBeInstanceOf(Error);
    expect(error.name).toBe("ApiError");
    expect(error.status).toBe(429);
    expect(error.message).toBe("Request failed (429)");
  });
});