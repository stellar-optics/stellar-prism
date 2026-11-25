"use client";

import React, {
  createContext,
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import type { WalletContextType, WalletStatus } from "../types/wallet";
import { getXlmBalance } from "../lib/stellar";
import {
  signAndSubmitTransaction,
  NetworkMismatchError,
} from "../lib/stellarTransactions";
import type { SignAndSubmitResult } from "../lib/stellarTransactions";
import {
  isNetworkMismatch,
  normalizeToPassphrase,
  getExpectedNetworkPassphrase,
} from "@/services/stellar/networkConfig";
import { trackWalletConnected, trackWalletDisconnected } from "@/lib/analytics";
import {
  WALLET_ADAPTERS,
  getPreferredAdapter,
  savePreferredAdapter,
  FreighterAdapter,
} from "../lib/walletAdapters";
import {
  authenticateWallet,
  logoutWalletSession,
  refreshAuthSession,
} from "@/lib/walletSession";
import {
  AUTH_EXPIRED_EVENT,
  getSessionWallet,
  hasValidAccessToken,
} from "@/lib/authToken";

const CONNECT_TIMEOUT_MS = 12_000;
const WALLET_POLL_INTERVAL_MS = 5_000;
const WALLET_EVENT_DEBOUNCE_MS = 500;
// Tolerate a few consecutive adapter read failures before tearing the
// session down. Freighter reads commonly throw transiently while a signing
// popup is open, and tearing down on the first error raced token issuance
// right after onboarding (#1185).
const RECONCILE_FAILURE_LIMIT = 3;
// After a popup closes, give the extension a moment before the poll reads it.
const POST_INTERACTION_QUIET_MS = 1_500;

/** The in-flight wallet interaction. Only one may run at a time. */
type Interaction = "restoring" | "connecting" | "authenticating";

const initialState: WalletContextType = {
  status: "disconnected",
  busy: false,
  pendingWalletId: null,
  address: null,
  balance: null,
  connected: false,
  loading: false,
  restoring: false,
  authenticated: false,
  authenticating: false,
  sessionError: null,
  error: null,
  network: null,
  networkMismatch: false,
  activeWalletId: null,
  connect: async () => {},
  disconnect: () => {},
  refreshBalance: async () => {},
  reauthenticate: async () => {},
  signAndSubmit: async () => ({
    success: false,
    error: "Wallet not connected",
  }),
};

export const WalletContext = createContext<WalletContextType>(initialState);

function clearWalletStorage() {
  localStorage.removeItem("walletAddress");
  localStorage.removeItem("walletNetwork");
  localStorage.removeItem("activeWalletId");
}

export const WalletProvider: React.FC<{ children: React.ReactNode }> = ({
  children,
}) => {
  const [address, setAddress] = useState<string | null>(null);
  const [balance, setBalance] = useState<string | null>(null);
  const [connected, setConnected] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [network, setNetwork] = useState<string | null>(null);
  const [activeWalletId, setActiveWalletId] = useState<string | null>(null);
  const [authenticated, setAuthenticated] = useState(false);
  const [sessionError, setSessionError] = useState<string | null>(null);
  // One flag for "what is the wallet doing right now" — replaces the separate
  // loading / restoring / authenticating booleans.
  // Starts null on both server and client (no hydration mismatch); the restore
  // effect flips it to "restoring" before first paint.
  const [interaction, setInteraction] = useState<Interaction | null>(null);
  const [pendingWalletId, setPendingWalletId] = useState<string | null>(null);

  const mountedRef = useRef(true);
  const generationRef = useRef(0);
  const restoreStartedRef = useRef(false);
  const lastWalletEventRef = useRef(0);
  const lastInteractionEndRef = useRef(0);
  const identitySyncRef = useRef(false);
  // The single wallet-interaction lock shared by restore, connect and
  // reauthenticate. The identity poll never runs while this is set, so it can
  // never read the extension while a connect/sign popup is open.
  const interactionRef = useRef<{
    kind: Interaction;
    promise: Promise<void>;
  } | null>(null);

  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  /**
   * Run `fn` as the one and only wallet interaction.
   *  - Same kind already in flight → share its promise (no second popup).
   *  - Different kind in flight → wait for it, then run.
   */
  const runInteraction = useCallback(
    (kind: Interaction, fn: () => Promise<void>): Promise<void> => {
      const current = interactionRef.current;
      if (current?.kind === kind) return current.promise;
      if (current) {
        return current.promise
          .catch(() => undefined)
          .then(() => runInteraction(kind, fn));
      }

      setInteraction(kind);
      const entry = { kind, promise: Promise.resolve() };
      entry.promise = (async () => {
        try {
          await fn();
        } finally {
          if (interactionRef.current === entry) {
            interactionRef.current = null;
            lastInteractionEndRef.current = Date.now();
            if (mountedRef.current) setInteraction(null);
          }
        }
      })();
      interactionRef.current = entry;
      return entry.promise;
    },
    [],
  );

  const resetWallet = useCallback(() => {
    setAddress(null);
    setBalance(null);
    setConnected(false);
    setNetwork(null);
    setActiveWalletId(null);
    clearWalletStorage();
  }, []);

  /** Adopt a still-valid (or silently refreshable) API session. Never prompts. */
  const adoptExistingSession = useCallback(
    async (wallet: string): Promise<boolean> => {
      if (hasValidAccessToken(wallet)) return true;
      const sessionWallet = getSessionWallet();
      if (!sessionWallet || sessionWallet.toLowerCase() !== wallet.toLowerCase()) {
        return false;
      }
      const refreshed = await refreshAuthSession();
      return refreshed === "refreshed" && hasValidAccessToken(wallet);
    },
    [],
  );

  // ── Restore on page load (silent reads only; runs once even in StrictMode) ──
  useEffect(() => {
    if (typeof window === "undefined" || restoreStartedRef.current) return;
    restoreStartedRef.current = true;

    const cachedAddr = localStorage.getItem("walletAddress");
    const cachedNet = localStorage.getItem("walletNetwork");
    const cachedWalletId = localStorage.getItem("activeWalletId");
    if (!cachedAddr) return;

    const generation = generationRef.current;
    const stale = () =>
      generation !== generationRef.current || !mountedRef.current;

    void runInteraction("restoring", async () => {
      const adapter =
        WALLET_ADAPTERS.find((a) => a.id === cachedWalletId) ??
        FreighterAdapter;
      try {
        // getPublicKey() is the silent read — it must not open a popup.
        const livePub = await adapter.getPublicKey();
        if (stale()) return;
        const liveNet = await adapter.getNetwork();
        if (stale()) return;

        if (livePub !== cachedAddr) localStorage.setItem("walletAddress", livePub);
        if (liveNet !== cachedNet) localStorage.setItem("walletNetwork", liveNet);

        setAddress(livePub);
        setNetwork(liveNet);
        setConnected(true);
        setActiveWalletId(adapter.id);

        getXlmBalance(livePub)
          .then((b) => {
            if (!stale()) setBalance(b);
          })
          .catch(() => undefined);

        // Restore the API session alongside the wallet connection so a page
        // reload right after onboarding doesn't drop a still-valid JWT (#1185).
        const adopted = await adoptExistingSession(livePub);
        if (stale()) return;
        if (adopted) setAuthenticated(true);
      } catch {
        if (stale()) return;
        resetWallet();
      }
    });
  }, [runInteraction, adoptExistingSession, resetWallet]);

  const refreshBalance = useCallback(async () => {
    try {
      const a = address;
      if (!a) return;
      const b = await getXlmBalance(a);
      if (mountedRef.current) setBalance(b);
    } catch (err: unknown) {
      const errorMsg = err instanceof Error ? err.message : String(err);
      console.error("Failed to fetch balance:", errorMsg);
      if (mountedRef.current) setError(errorMsg);
    }
  }, [address]);

  // ── Connect (the only path allowed to open the authorization popup) ──
  const connect = useCallback(
    (adapterId?: string): Promise<void> => {
      // Already connected, or a connect/restore is running → never re-prompt.
      // Double clicks, StrictMode remounts and multiple Connect buttons all
      // collapse onto the same in-flight promise (#1160).
      const current = interactionRef.current;
      if (current && current.kind !== "authenticating") return current.promise;

      const adapter =
        (adapterId ? WALLET_ADAPTERS.find((a) => a.id === adapterId) : null) ??
        getPreferredAdapter();

      return runInteraction("connecting", async () => {
        const generation = ++generationRef.current;
        const stale = () =>
          generation !== generationRef.current || !mountedRef.current;
        setError(null);
        setPendingWalletId(adapter.id);

        try {
          const isMobile =
            typeof navigator !== "undefined" &&
            /android|iphone|ipad|ipod|mobile/i.test(navigator.userAgent);
          if (isMobile && !adapter.supportsMobile()) {
            const deepLink = adapter.mobileDeepLink();
            setError(
              deepLink
                ? `Open ${adapter.name} at ${deepLink} and try again.`
                : `${adapter.name} is not supported on mobile. Please use a desktop browser with the ${adapter.name} extension installed.`,
            );
            return;
          }

          let timer: ReturnType<typeof setTimeout> | undefined;
          const pub = await Promise.race([
            adapter.connect(),
            new Promise<never>((_, reject) => {
              timer = setTimeout(
                () =>
                  reject(
                    new Error(
                      `Connection timed out after ${CONNECT_TIMEOUT_MS / 1000}s. ` +
                        `Make sure ${adapter.name} is unlocked and try again.`,
                    ),
                  ),
                CONNECT_TIMEOUT_MS,
              );
            }),
          ]).finally(() => clearTimeout(timer));
          if (stale()) return;

          const networkName = await adapter.getNetwork();
          if (stale()) return;

          // A different account than the one the API session belongs to must
          // not inherit it.
          if (address && address !== pub) {
            setAuthenticated(false);
          }

          setAddress(pub);
          setNetwork(networkName);
          setConnected(true);
          setActiveWalletId(adapter.id);
          trackWalletConnected(pub, {
            network: networkName,
            adapter: adapter.name,
          });

          localStorage.setItem("walletAddress", pub);
          localStorage.setItem("walletNetwork", networkName);
          localStorage.setItem("activeWalletId", adapter.id);
          savePreferredAdapter(adapter.id);

          // Adopt an existing session silently so the user isn't asked to sign
          // again right after connecting.
          const adopted = await adoptExistingSession(pub);
          if (stale()) return;
          if (adopted) {
            setSessionError(null);
            setAuthenticated(true);
          }

          getXlmBalance(pub)
            .then((b) => {
              if (!stale()) setBalance(b);
            })
            .catch(() => undefined);
        } catch (err: unknown) {
          if (stale()) return;
          setError(err instanceof Error ? err.message : String(err));
          setConnected(false);
          setAddress(null);
          setBalance(null);
          setNetwork(null);
          setActiveWalletId(null);
        } finally {
          if (mountedRef.current) setPendingWalletId(null);
        }
      });
    },
    [address, runInteraction, adoptExistingSession],
  );

  const clearAuthenticatedSession = useCallback(() => {
    setAuthenticated(false);
    setSessionError(null);
    void logoutWalletSession().catch((err) => {
      console.error("Wallet logout failed:", err);
    });
    if (typeof window !== "undefined") {
      window.dispatchEvent(new Event("walletIdentityChanged"));
    }
  }, []);

  const disconnect = useCallback(() => {
    // Invalidate any in-flight restore/connect/sign-in result.
    generationRef.current += 1;
    if (address) {
      trackWalletDisconnected({
        network: network ?? undefined,
        adapter: activeWalletId ?? undefined,
      });
    }
    clearAuthenticatedSession();
    resetWallet();
    setError(null);
    setPendingWalletId(null);
    // Drop the UI phase now; the lock itself releases when the pending
    // promise settles, and its result is discarded by the generation bump.
    setInteraction(null);
  }, [address, network, activeWalletId, clearAuthenticatedSession, resetWallet]);

  // ── Sign-in (the only path allowed to open the signature popup) ──
  const reauthenticate = useCallback((): Promise<void> => {
    if (!address || !activeWalletId) {
      return Promise.reject(new Error("Connect a wallet before signing in."));
    }
    const adapter = WALLET_ADAPTERS.find((item) => item.id === activeWalletId);
    if (!adapter) {
      return Promise.reject(
        new Error("No wallet adapter is active. Reconnect your wallet."),
      );
    }

    // A still-valid session doesn't need another signature — silently adopt
    // it so completing onboarding (or opening the gate) never re-prompts.
    if (hasValidAccessToken(address)) {
      setSessionError(null);
      setAuthenticated(true);
      return Promise.resolve();
    }

    const wallet = address;
    // Gate, onboarding, account menu and product pages may all call this;
    // they share one signature request (and its success/failure) instead of
    // each opening Freighter.
    return runInteraction("authenticating", async () => {
      const generation = generationRef.current;
      const stale = () =>
        generation !== generationRef.current || !mountedRef.current;
      setSessionError(null);
      try {
        if (await adoptExistingSession(wallet)) {
          if (!stale()) setAuthenticated(true);
          return;
        }
        if (stale()) return;
        await authenticateWallet(adapter, wallet);
        if (!stale()) setAuthenticated(true);
      } catch (err) {
        if (stale()) return;
        setAuthenticated(false);
        // Don't brand a transient network blip as an expired session (#1185).
        const isNetworkError =
          err instanceof TypeError ||
          (err instanceof Error &&
            /failed to fetch|networkerror|fetch failed/i.test(err.message));
        setSessionError(
          isNetworkError ? null : err instanceof Error ? err.message : String(err),
        );
        throw err instanceof Error ? err : new Error(String(err));
      }
    });
  }, [activeWalletId, address, runInteraction, adoptExistingSession]);

  // ── Identity poll (silent reads; paused during any interaction) ──
  useEffect(() => {
    if (
      !connected ||
      !activeWalletId ||
      !address ||
      typeof window === "undefined"
    ) {
      return;
    }

    const adapter = WALLET_ADAPTERS.find((item) => item.id === activeWalletId);
    if (!adapter) return;
    let cancelled = false;
    let consecutiveFailures = 0;

    const reconcileIdentity = async () => {
      if (cancelled || identitySyncRef.current) return;
      // Never touch the extension while a connect/sign popup is open, right
      // after one closed, or while the tab is hidden.
      if (interactionRef.current) return;
      if (Date.now() - lastInteractionEndRef.current < POST_INTERACTION_QUIET_MS) return;
      if (typeof document !== "undefined" && document.visibilityState === "hidden") return;

      identitySyncRef.current = true;
      const generation = generationRef.current;
      try {
        const [liveAddress, liveNetwork] = await Promise.all([
          adapter.getPublicKey(),
          adapter.getNetwork(),
        ]);
        if (cancelled || !mountedRef.current) return;
        // An interaction started while we were reading — its result wins.
        if (interactionRef.current || generation !== generationRef.current) return;
        consecutiveFailures = 0;

        const accountChanged = liveAddress !== address;
        const networkChanged = liveNetwork !== network;
        if (!accountChanged && !networkChanged) return;

        clearAuthenticatedSession();
        setBalance(null);
        setAddress(liveAddress);
        setNetwork(liveNetwork);
        localStorage.setItem("walletAddress", liveAddress);
        localStorage.setItem("walletNetwork", liveNetwork);
        setError(
          accountChanged
            ? "Wallet account changed. Previous wallet data and transaction intent were cleared."
            : "Wallet network changed. Previous wallet data and transaction intent were cleared.",
        );

        try {
          const liveBalance = await getXlmBalance(liveAddress);
          if (!cancelled && mountedRef.current) setBalance(liveBalance);
        } catch {
          // Balance refresh is non-fatal after an identity change.
        }
      } catch {
        if (cancelled || !mountedRef.current) return;
        if (interactionRef.current || generation !== generationRef.current) return;
        consecutiveFailures += 1;
        // Transient adapter errors (locked extension, popup mid-sign, network
        // blip) must not destroy a freshly-issued session — only a sustained
        // failure tears down the wallet (#1185).
        if (consecutiveFailures < RECONCILE_FAILURE_LIMIT) return;
        clearAuthenticatedSession();
        setError("Wallet is locked or disconnected. Please reconnect.");
        resetWallet();
      } finally {
        identitySyncRef.current = false;
      }
    };

    const handleProviderEvent = () => {
      const now = Date.now();
      if (now - lastWalletEventRef.current < WALLET_EVENT_DEBOUNCE_MS) return;
      lastWalletEventRef.current = now;
      void reconcileIdentity();
    };
    const handleVisibility = () => {
      if (document.visibilityState === "visible") void reconcileIdentity();
    };
    const provider = (window as Window & { freighter?: unknown }).freighter as
      | {
          on?: (event: string, listener: () => void) => void;
          removeListener?: (event: string, listener: () => void) => void;
        }
      | undefined;
    const providerEvents = ["accountsChanged", "chainChanged", "disconnect"];
    const windowEvents = [
      "accountsChanged",
      "chainChanged",
      "walletChanged",
      "walletDisconnected",
    ];
    providerEvents.forEach((event) =>
      provider?.on?.(event, handleProviderEvent),
    );
    windowEvents.forEach((event) =>
      window.addEventListener(event, handleProviderEvent),
    );
    document.addEventListener("visibilitychange", handleVisibility);
    const interval = window.setInterval(
      reconcileIdentity,
      WALLET_POLL_INTERVAL_MS,
    );

    return () => {
      cancelled = true;
      window.clearInterval(interval);
      providerEvents.forEach((event) =>
        provider?.removeListener?.(event, handleProviderEvent),
      );
      windowEvents.forEach((event) =>
        window.removeEventListener(event, handleProviderEvent),
      );
      document.removeEventListener("visibilitychange", handleVisibility);
    };
  }, [
    activeWalletId,
    address,
    connected,
    network,
    clearAuthenticatedSession,
    resetWallet,
  ]);

  // The api layer dispatches AUTH_EXPIRED_EVENT when it clears an expired or
  // rejected session; flip the auth state here so the wallet gate and every
  // consumer sees it immediately instead of after a failed request (#1185).
  useEffect(() => {
    const handleAuthExpired = () => {
      if (!mountedRef.current) return;
      setAuthenticated(false);
      setSessionError("Your session has expired. Please sign in again.");
    };
    window.addEventListener(AUTH_EXPIRED_EVENT, handleAuthExpired);
    return () => {
      window.removeEventListener(AUTH_EXPIRED_EVENT, handleAuthExpired);
    };
  }, []);

  // Compare the connected wallet's active network against the network the app
  // is configured for. Recomputed whenever the wallet reports a network change.
  const networkMismatch = useMemo(
    () => (connected ? isNetworkMismatch(network) : false),
    [connected, network],
  );

  const signAndSubmit = useCallback(
    async (transactionXdr: string): Promise<SignAndSubmitResult> => {
      if (!connected || !address) {
        return { success: false, error: "Wallet not connected" };
      }
      if (isNetworkMismatch(network)) {
        let expected = "the configured network";
        try {
          expected = getExpectedNetworkPassphrase();
        } catch {
          /* keep generic label */
        }
        const err = new NetworkMismatchError(
          normalizeToPassphrase(network) ?? network ?? "unknown",
          expected,
        );
        if (mountedRef.current) setError(err.message);
        return { success: false, error: err.message, errorKind: "mismatch" };
      }
      setError(null);
      try {
        const result = await signAndSubmitTransaction(transactionXdr);
        if (result.success) {
          const b = await getXlmBalance(address);
          if (mountedRef.current) setBalance(b);
        }
        return result;
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        if (mountedRef.current) setError(msg);
        return { success: false, error: msg };
      }
    },
    [connected, address, network],
  );

  const status: WalletStatus =
    interaction ??
    (connected ? (authenticated ? "ready" : "connected") : "disconnected");

  const value = useMemo<WalletContextType>(
    () => ({
      status,
      busy: interaction !== null,
      pendingWalletId,
      address,
      balance,
      connected,
      loading: status === "connecting",
      restoring: status === "restoring",
      authenticating: status === "authenticating",
      error,
      network,
      networkMismatch,
      activeWalletId,
      authenticated,
      sessionError,
      connect,
      disconnect,
      refreshBalance,
      reauthenticate,
      signAndSubmit,
    }),
    [
      status,
      interaction,
      pendingWalletId,
      address,
      balance,
      connected,
      error,
      network,
      networkMismatch,
      activeWalletId,
      authenticated,
      sessionError,
      connect,
      disconnect,
      refreshBalance,
      reauthenticate,
      signAndSubmit,
    ],
  );

  return (
    <WalletContext.Provider value={value}>{children}</WalletContext.Provider>
  );
};
