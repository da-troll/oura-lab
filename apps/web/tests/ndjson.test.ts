import { describe, expect, it } from "vitest";

import { splitNdjsonBuffer } from "../src/lib/ndjson";

describe("splitNdjsonBuffer", () => {
  it("returns complete lines and carries trailing buffer", () => {
    const result = splitNdjsonBuffer("", '{"a":1}\n{"b":2}\n{"c":');
    expect(result.lines).toEqual(['{"a":1}', '{"b":2}']);
    expect(result.buffer).toBe('{"c":');
  });

  it("reconstructs lines across chunks", () => {
    const first = splitNdjsonBuffer("", '{"type":"progress","percent":4');
    expect(first.lines).toEqual([]);
    expect(first.buffer).toBe('{"type":"progress","percent":4');

    const second = splitNdjsonBuffer(
      first.buffer,
      '2}\n{"type":"done"}\n'
    );
    expect(second.lines).toEqual([
      '{"type":"progress","percent":42}',
      '{"type":"done"}',
    ]);
    expect(second.buffer).toBe("");
  });
});
