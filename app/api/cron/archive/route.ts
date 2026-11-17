export async function GET() {
  return Response.json(
    {
      ok: false,
      error: "gone",
      message: "The cold-storage archive endpoint has been removed.",
    },
    { status: 410 }
  );
}
