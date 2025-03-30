import { bench, describe } from 'vitest';
import {
  stringFromBlob,
  stringFromBlobArrayJoin,
  stringFromBlobStringConcat,
} from '../../src/conversion/stringFromBlob';

function createBytes(length: number): Uint8Array {
  const bytes = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    bytes[i] = i % 256;
  }
  return bytes;
}

for (let lenPow2 = 0; lenPow2 < 24; lenPow2++) {
  const bytes = createBytes(2 ** lenPow2);

  describe(`stringFromBlob ${lenPow2}`, () => {
    bench('string concat', () => {
      stringFromBlobStringConcat(bytes);
    });
    bench('array join', () => {
      stringFromBlobArrayJoin(bytes);
    });
    bench('combined', () => {
      stringFromBlob(bytes);
    });
  });
}
