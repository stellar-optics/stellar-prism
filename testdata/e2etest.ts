import { useEffect, useState } from "react";

// ASSUMPTION: adjust to match the real shape returned by GET /orders
interface BuyerOrder {
  id: string;
  status: string;
  createdAt: string; // ISO date
  campaign?: { id?: string; name?: string; title?: string } | null;
  campaignName?: string;
}

interface BuyerDashboardProps {
  buyerAddress: string;
  apiBaseUrl?: string; // e.g. import.meta.env.VITE_API_URL
  browseHref?: string;
}

type State =
  | { kind: "loading" }
  | { kind: "error"; message: string }
  | { kind: "ready"; orders: BuyerOrder[] };

function campaignLabel(o: BuyerOrder): string {
  return o.campaign?.name ?? o.campaign?.title ?? o.campaignName ?? "Unknown campaign";
}

export default function BuyerDashboard({
  buyerAddress,
  apiBaseUrl = "",
  browseHref = "/campaigns",
}: BuyerDashboardProps) {
  const [state, setState] = useState<State>({ kind: "loading" });
  const [attempt, setAttempt] = useState(0);

  // Buyer-only: fetches orders for this address and nothing investor-related.
  useEffect(() => {
    if (!buyerAddress) return;
    const controller = new AbortController();
    setState({ kind: "loading" });

    fetch(`${apiBaseUrl}/orders?buyerAddress=${encodeURIComponent(buyerAddress)}`, {
      signal: controller.signal,
    })
      .then(async (res) => {
        if (!res.ok) throw new Error(`Request failed (${res.status})`);
        const data = await res.json();
        const orders: BuyerOrder[] = Array.isArray(data) ? data : data.orders ?? [];
        setState({ kind: "ready", orders });
      })
      .catch((err) => {
        if (err.name === "AbortError") return;
        setState({ kind: "error", message: err.message || "Could not load orders." });
      });

    return () => controller.abort();
  }, [buyerAddress, apiBaseUrl, attempt]);

  return (
    <section aria-labelledby="buyer-orders-heading">
      <h2 id="buyer-orders-heading">Your orders</h2>

      {state.kind === "loading" && <p role="status">Loading your orders…</p>}

      {state.kind === "error" && (
        <div role="alert">
          <p>{state.message}</p>
          <button type="button" onClick={() => setAttempt((n) => n + 1)}>
            Try again
          </button>
        </div>
      )}

      {state.kind === "ready" && state.orders.length === 0 && (
        <div>
          <p>You haven't placed any orders yet.</p>
          <a href={browseHref}>Browse campaigns</a>
        </div>
      )}

      {state.kind === "ready" && state.orders.length > 0 && (
        <>
          <table>
            <thead>
              <tr>
                <th>Campaign</th>
                <th>Status</th>
                <th>Date</th>
              </tr>
            </thead>
            <tbody>
              {state.orders.map((o) => (
                <tr key={o.id}>
                  <td>{campaignLabel(o)}</td>
                  <td>{o.status}</td>
                  <td>{new Date(o.createdAt).toLocaleDateString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
          <a href={browseHref}>Browse campaigns</a>
        </>
      )}
    </section>
  );
}
