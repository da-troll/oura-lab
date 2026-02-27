export function splitNdjsonBuffer(
  existingBuffer: string,
  chunk: string
): { lines: string[]; buffer: string } {
  const merged = existingBuffer + chunk;
  const parts = merged.split("\n");
  const buffer = parts.pop() ?? "";
  const lines = parts.filter((line) => line.trim().length > 0);
  return { lines, buffer };
}
