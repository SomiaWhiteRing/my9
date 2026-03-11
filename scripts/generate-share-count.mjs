import {
  buildDatabaseUrlFromNeonParts,
  resolveShareCountFromDatabase,
} from "./share-count-utils.mjs";

async function generateShareCount() {
  const databaseUrl = buildDatabaseUrlFromNeonParts();
  if (!databaseUrl) {
    console.log("NEXT_PUBLIC_SHARE_COUNT=0");
    return;
  }

  try {
    const totalCount = await resolveShareCountFromDatabase(databaseUrl);
    console.log(`NEXT_PUBLIC_SHARE_COUNT=${totalCount}`);
  } catch (error) {
    console.error("Failed to generate share count:", error);
    console.log("NEXT_PUBLIC_SHARE_COUNT=0");
  }
}

generateShareCount();
