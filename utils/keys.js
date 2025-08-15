import { createPublicKey, createHash } from "crypto";

/**
 * Return a value suitable for snowflake-sdk's `privateKey` option:
 *  - a PEM string for unencrypted keys, or
 *  - { key: PEM, passphrase } for encrypted keys.
 * Accepts either SNOWFLAKE_PRIVATE_KEY_B64 (preferred) or SNOWFLAKE_PRIVATE_KEY.
 */
export function getSnowflakePrivateKeyParam() {
  const b64 = process.env.SNOWFLAKE_PRIVATE_KEY_B64 || "";
  if (b64.trim()) {
    const pem = Buffer.from(b64, "base64").toString("utf8");
    validatePkcs8Pem(pem, "SNOWFLAKE_PRIVATE_KEY_B64");
    return withPassphrase(pem);
  }

  // Fallback to raw PEM env; support literal "\n" sequences
  let pem = process.env.SNOWFLAKE_PRIVATE_KEY || "";
  pem = pem.replace(/\\n/g, "\n");
  validatePkcs8Pem(pem, "SNOWFLAKE_PRIVATE_KEY");
  return withPassphrase(pem);
}

function withPassphrase(pem) {
  const pass = process.env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE;
  if (pass && pass.length > 0) {
    return { key: pem, passphrase: pass };
  }
  return pem;
}

function validatePkcs8Pem(pem, sourceName) {
  if (!pem || !/BEGIN (ENCRYPTED )?PRIVATE KEY/.test(pem)) {
    throw new Error(
      `${sourceName} must be a PKCS#8 PEM starting with "-----BEGIN PRIVATE KEY-----" or "-----BEGIN ENCRYPTED PRIVATE KEY-----"`
    );
  }
}

/** Compute SHA-256 (base64) fingerprint of the public key derived from a PEM private key. */
export function computePrivateKeyFingerprint(pemOrObj) {
  const pem = typeof pemOrObj === "string" ? pemOrObj : pemOrObj?.key || "";
  const der = createPublicKey({ key: pem, format: "pem" }).export({ type: "spki", format: "der" });
  return createHash("sha256").update(der).digest("base64");
}

/** Human-friendly source string for logs */
export function detectKeySource() {
  if ((process.env.SNOWFLAKE_PRIVATE_KEY_B64 || "").trim()) return "B64";
  if ((process.env.SNOWFLAKE_PRIVATE_KEY || "").trim()) return "RAW";
  return "NONE";
}
