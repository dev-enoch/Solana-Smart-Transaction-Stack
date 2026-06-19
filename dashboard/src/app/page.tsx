"use client";

import { useEffect, useState, Suspense } from "react";
import { useRouter, useSearchParams, usePathname } from "next/navigation";
import { Search, Inbox, LayoutDashboard, Activity, Zap, X, Menu } from 'lucide-react';



type Stats = {
  lifecycle: {
    totalTx: number;
    finalized: number;
    failed: number;
    totalTip: number;
  };
  operational: {
    totalEvents: number;
    submissions: number;
    failures: number;
    retries: number;
  };
} | null;

type LogEntry = any;

function DashboardContent() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [activeTab, setActiveTab] = useState<"lifecycle" | "operational">(
    (searchParams.get("tab") as "lifecycle" | "operational") || "lifecycle"
  );
  const [currentPage, setCurrentPage] = useState(
    parseInt(searchParams.get("page") || "1", 10)
  );
  const [searchQuery, setSearchQuery] = useState(searchParams.get("search") || "");
  const [searchInput, setSearchInput] = useState(searchQuery);

  const [logs, setLogs] = useState<LogEntry[]>([]);
  const [stats, setStats] = useState<Stats | null>(null);
  
  const [totalPages, setTotalPages] = useState(1);
  const [totalItems, setTotalItems] = useState(0);
  const [initialLoad, setInitialLoad] = useState(true);
  const [network, setNetwork] = useState<string>("mainnet");
  
  const [selectedLog, setSelectedLog] = useState<LogEntry | null>(null);

  const limit = 10;

  // Sync URL when state changes
  useEffect(() => {
    const params = new URLSearchParams(searchParams.toString());
    params.set("tab", activeTab);
    params.set("page", currentPage.toString());
    if (searchQuery) {
      params.set("search", searchQuery);
    } else {
      params.delete("search");
    }
    
    const newQuery = params.toString();
    if (newQuery !== searchParams.toString()) {
      router.replace(`${pathname}?${newQuery}`, { scroll: false });
    }
  }, [activeTab, currentPage, searchQuery, pathname, router, searchParams]);



  const fetchLogs = async (page: number, type: string, search: string) => {
    try {
      const res = await fetch(`/api/logs/${type}?page=${page}&limit=${limit}&search=${encodeURIComponent(search)}`);
      const data = await res.json();
      if (data.network) setNetwork(data.network);
      if (data.stats) {
        setStats((prev) => ({
          ...(prev || {} as any),
          ...data.stats
        }));
      }
      if (data.data) {
        setLogs(data.data);
        setTotalPages(data.totalPages || 1);
        setTotalItems(data.totalItems || 0);
      }
    } catch (e) {
      console.error(e);
    } finally {
      setInitialLoad(false);
    }
  };

  useEffect(() => {
    fetchLogs(currentPage, activeTab, searchQuery);
    
    const eventSource = new EventSource('/api/logs/stream');

    eventSource.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data);
        
        if (currentPage === 1 && !searchQuery) {
          if (data.type === 'lifecycle_update' && activeTab === 'lifecycle') {
            fetchLogs(1, 'lifecycle', '');
          } else if (data.type === 'operational_update' && activeTab === 'operational') {
            fetchLogs(1, 'operational', '');
          }
        }
      } catch (err) {}
    };

    return () => eventSource.close();
  }, [currentPage, activeTab, searchQuery]);

  // Lock body scroll when modal is open
  useEffect(() => {
    if (selectedLog) {
      document.body.style.overflow = "hidden";
    } else {
      document.body.style.overflow = "";
    }
    return () => {
      document.body.style.overflow = "";
    };
  }, [selectedLog]);

  const handleTabChange = (tab: "lifecycle" | "operational") => {
    setActiveTab(tab);
    setCurrentPage(1);
    setSearchQuery("");
    setSearchInput("");
    setSelectedLog(null);
    setLogs([]);
    setSidebarOpen(false);
  };

  const handleFilterChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const val = e.target.value;
    setSearchInput(val);
    setSearchQuery(val);
    setCurrentPage(1);
  };

  const handleClearFilter = () => {
    setSearchInput("");
    setSearchQuery("");
    setCurrentPage(1);
  };

  const getSolanaUrl = (path: string) => {
    return network === "devnet" 
      ? `https://explorer.solana.com${path}?cluster=devnet` 
      : `https://explorer.solana.com${path}`;
  };

  return (
    <div className="app-layout">
      {/* Mobile Sidebar Overlay */}
      {sidebarOpen && (
        <div className="sidebar-backdrop" onClick={() => setSidebarOpen(false)}></div>
      )}

      {/* Sidebar */}
      <aside className={`sidebar ${sidebarOpen ? "open" : ""}`}>
        <div className="sidebar-header">
          <div className="sidebar-title">Smart Tx Stack</div>
          <button className="btn-close-sidebar" onClick={() => setSidebarOpen(false)}>
            <X size={18} />
          </button>
        </div>
        <div className="nav-menu">
          <div 
            className={`nav-item ${activeTab === "lifecycle" ? "active" : ""}`}
            onClick={() => handleTabChange("lifecycle")}
          >
            <Activity size={18} /> Lifecycle Logs
          </div>
          <div 
            className={`nav-item ${activeTab === "operational" ? "active" : ""}`}
            onClick={() => handleTabChange("operational")}
          >
            <Zap size={18} /> Operational Events
          </div>
        </div>
      </aside>

      {/* Mobile Navigation Header */}
      <div className="mobile-navbar">
        <button className="btn-menu" onClick={() => setSidebarOpen(true)}>
          <Menu size={22} />
        </button>
        <div className="mobile-navbar-title">Smart Tx Stack</div>
      </div>

      {/* Main Content */}
      <main className="main-content">
        <header>
          <h1 style={{ display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
            <LayoutDashboard size={28} /> Dashboard
          </h1>
          <p className="subtitle">Real-time performance monitoring</p>
        </header>

        {stats?.lifecycle && activeTab === "lifecycle" && (
          <section className="stats-grid">
            <div className="card stat-card success">
              <div className="stat-title">Success Rate</div>
              <div className="stat-value">
                {stats.lifecycle.totalTx > 0 ? Math.round((stats.lifecycle.finalized / stats.lifecycle.totalTx) * 100) : 0}%
              </div>
            </div>
            <div className="card stat-card">
              <div className="stat-title">Total Txs</div>
              <div className="stat-value">{stats.lifecycle.totalTx}</div>
            </div>
            <div className="card stat-card">
              <div className="stat-title">Finalized</div>
              <div className="stat-value success">{stats.lifecycle.finalized}</div>
            </div>
            <div className="card stat-card">
              <div className="stat-title">Avg Tip (SOL)</div>
              <div className="stat-value accent">
                {stats.lifecycle.totalTx > 0 ? (stats.lifecycle.totalTip / stats.lifecycle.totalTx / 1e9).toFixed(6) : "0"}
              </div>
            </div>
          </section>
        )}

        {stats?.operational && activeTab === "operational" && (
          <section className="stats-grid">
            <div className="card stat-card">
              <div className="stat-title">Total Events</div>
              <div className="stat-value">{stats.operational.totalEvents}</div>
            </div>
            <div className="card stat-card">
              <div className="stat-title">Submissions</div>
              <div className="stat-value accent">{stats.operational.submissions}</div>
            </div>
            <div className="card stat-card">
              <div className="stat-title">Failures Detected</div>
              <div className="stat-value" style={{color: "var(--error-color)"}}>{stats.operational.failures}</div>
            </div>
            <div className="card stat-card">
              <div className="stat-title">Retries Queued</div>
              <div className="stat-value" style={{color: "var(--warning-color)"}}>{stats.operational.retries}</div>
            </div>
          </section>
        )}

        <section className="card">
          <div className="section-header">
            <h2 className="section-title" style={{marginBottom: 0}}>
              {activeTab === "lifecycle" ? "Recent Transactions" : "Operational Events"}
            </h2>
            <div className="search-container">
              <select
                className="search-input"
                value={searchInput}
                onChange={handleFilterChange}
              >
                <option value="">All Statuses</option>
                {activeTab === "lifecycle" ? (
                  <>
                    <option value="pending">Pending</option>
                    <option value="processed">Processed</option>
                    <option value="confirmed">Confirmed</option>
                    <option value="finalized">Finalized</option>
                    <option value="failed">Failed</option>
                  </>
                ) : (
                  <>
                    <option value="bundle_submitted">Bundle Submitted</option>
                    <option value="failure_detected">Failure Detected</option>
                    <option value="retry_queued">Retry Queued</option>
                    <option value="submission_error">Submission Error</option>
                  </>
                )}
              </select>
              <button className="btn-clear" onClick={handleClearFilter}>Clear</button>
            </div>
          </div>
          
          {initialLoad ? (
            <div className="loader">Loading data...</div>
          ) : (
            <>
              <div className="list-container">
                {logs.map((log, idx) => {
                  if (activeTab === "lifecycle") {
                    const sigShort = log.signatures?.[0]
                      ? `${log.signatures[0].slice(0, 8)}...${log.signatures[0].slice(-8)}`
                      : log.bundle_id?.slice(0, 16);
                    
                    return (
                      <div key={log.bundle_id || idx} className="list-item" onClick={() => setSelectedLog(log)}>
                        <div className="item-main">
                          <div className="item-id">
                            {log.signatures?.[0] ? (
                              <a 
                                href={getSolanaUrl(`/tx/${log.signatures[0]}`)}
                                target="_blank"
                                rel="noreferrer"
                                style={{ color: "inherit", textDecoration: "underline", textDecorationStyle: "dotted" }}
                                onClick={(e) => e.stopPropagation()}
                              >
                                {sigShort}
                              </a>
                            ) : (
                              sigShort
                            )}
                          </div>
                          <div className="item-meta">
                            <span>
                              Slot: <a 
                                href={getSolanaUrl(`/block/${log.slot_submitted}`)}
                                target="_blank"
                                rel="noreferrer"
                                style={{ color: "inherit", textDecoration: "underline", textDecorationStyle: "dotted" }}
                                onClick={(e) => e.stopPropagation()}
                              >
                                {log.slot_submitted}
                              </a>
                            </span>
                            <span>Tip: {(log.tip_lamports / 1e9).toFixed(6)} SOL</span>
                            {log.latency_finalized_ms && (
                              <span>Latency: {log.latency_finalized_ms}ms</span>
                            )}
                            {log.status === "failed" && log.failure_type && (
                              <span style={{color: "var(--status-error)"}}>
                                Reason: {log.failure_type.replace(/_/g, " ")}
                              </span>
                            )}
                          </div>
                        </div>
                        <div>
                          <span className={`badge ${log.status}`}>
                            {log.status}
                          </span>
                        </div>
                      </div>
                    );
                  } else {
                    // Operational event
                    return (
                      <div key={idx} className="list-item" onClick={() => setSelectedLog(log)}>
                        <div className="item-main">
                          <div className="item-id">{log.event?.replace(/_/g, " ")?.toUpperCase() || "UNKNOWN"}</div>
                          <div className="item-meta">
                            <span>{log.timestamp ? new Date(log.timestamp).toLocaleString() : ""}</span>
                            {log.bundle_id && (
                              <span>
                                Bundle: <a 
                                  href={getSolanaUrl(`/tx/${log.bundle_id}`)}
                                  target="_blank"
                                  rel="noreferrer"
                                  style={{ color: "inherit", textDecoration: "underline", textDecorationStyle: "dotted" }}
                                  onClick={(e) => e.stopPropagation()}
                                >
                                  {log.bundle_id.slice(0, 8)}...
                                </a>
                              </span>
                            )}
                            {log.details && <span>{log.details}</span>}
                          </div>
                        </div>
                        <div>
                          <span className={`badge ${log.event || ""}`}>
                            {log.event?.replace(/_/g, " ") || "UNKNOWN"}
                          </span>
                        </div>
                      </div>
                    );
                  }
                })}
                {logs.length === 0 && (
                  <div style={{padding: "4rem 2rem", textAlign: "center", color: "var(--text-muted)", display: "flex", flexDirection: "column", gap: "0.5rem", alignItems: "center"}}>
                    {searchQuery ? (
                      <>
                        <div style={{marginBottom: "0.5rem", color: "var(--text-muted)"}}>
                          <Search size={48} strokeWidth={1.5} />
                        </div>
                        <div style={{fontWeight: 500, color: "var(--text-primary)", fontSize: "1.1rem"}}>No results found</div>
                        <div>We couldn't find any logs matching your filter criteria.</div>
                      </>
                    ) : (
                      <>
                        <div style={{marginBottom: "0.5rem", color: "var(--text-muted)"}}>
                          <Inbox size={48} strokeWidth={1.5} />
                        </div>
                        <div style={{fontWeight: 500, color: "var(--text-primary)", fontSize: "1.1rem"}}>No Data Available</div>
                        <div>{activeTab === "lifecycle" ? "There are no transaction logs available yet." : "There are no operational events available yet."}</div>
                        <div style={{fontSize: "0.85rem", marginTop: "0.5rem"}}>Start sending transactions via the stack to generate logs.</div>
                      </>
                    )}
                  </div>
                )}
              </div>

              {/* Pagination */}
              {totalPages > 1 && (
                <div className="pagination">
                  <div className="pagination-info">
                    Showing {Math.min((currentPage - 1) * limit + 1, totalItems)} to {Math.min(currentPage * limit, totalItems)} of {totalItems} items
                  </div>
                  <div className="pagination-controls">
                    <button 
                      className="btn-page" 
                      disabled={currentPage === 1}
                      onClick={() => setCurrentPage(p => Math.max(1, p - 1))}
                    >
                      Previous
                    </button>
                    <button 
                      className="btn-page" 
                      disabled={currentPage === totalPages}
                      onClick={() => setCurrentPage(p => Math.min(totalPages, p + 1))}
                    >
                      Next
                    </button>
                  </div>
                </div>
              )}
            </>
          )}
        </section>
      </main>

      {/* Slide-Over Modal */}
      {selectedLog && (
        <>
          <div className="slide-over-overlay" onClick={() => setSelectedLog(null)}></div>
          <div className="slide-over">
            <div className="slide-over-header">
              <div className="slide-over-title">
                {activeTab === "lifecycle" ? "Transaction Details" : "Event Details"}
              </div>
              <button className="btn-close" onClick={() => setSelectedLog(null)}>
                <X size={20} />
              </button>
            </div>
            <div className="slide-over-content">
              {Object.entries(selectedLog).map(([key, value]) => {
                let displayValue: React.ReactNode = "";
                
                if (key === "signatures" && Array.isArray(value)) {
                  displayValue = (
                    <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                      {value.map((sig: string) => (
                        <a 
                          key={sig} 
                          href={getSolanaUrl(`/tx/${sig}`)} 
                          target="_blank" 
                          rel="noreferrer"
                          style={{ color: "var(--accent-color)", textDecoration: "none" }}
                        >
                          {sig} ↗
                        </a>
                      ))}
                    </div>
                  );
                } else if ((key === "slot" || key === "slot_submitted") && typeof value === "number") {
                  displayValue = (
                    <a 
                      href={getSolanaUrl(`/block/${value}`)} 
                      target="_blank" 
                      rel="noreferrer"
                      style={{ color: "var(--accent-color)", textDecoration: "none" }}
                    >
                      {value} ↗
                    </a>
                  );
                } else if ((key === "bundle_id" || key === "original_bundle_id") && typeof value === "string") {
                  displayValue = (
                    <a 
                      href={getSolanaUrl(`/tx/${value}`)} 
                      target="_blank" 
                      rel="noreferrer"
                      style={{ color: "var(--accent-color)", textDecoration: "none" }}
                    >
                      {value} ↗
                    </a>
                  );
                } else if (typeof value === "object" && value !== null) {
                  displayValue = JSON.stringify(value, null, 2);
                } else if (key === "timestamp" && typeof value === "string") {
                  displayValue = new Date(value).toLocaleString();
                } else if (key === "tip_lamports" && typeof value === "number") {
                  displayValue = `${(value / 1e9).toFixed(6)} SOL (${value} lamports)`;
                } else {
                  displayValue = String(value);
                }

                return (
                  <div key={key} className="detail-row">
                    <div className="detail-key">{key.replace(/_/g, " ")}</div>
                    <div className="detail-value">{displayValue}</div>
                  </div>
                );
              })}
            </div>
          </div>
        </>
      )}
    </div>
  );
}

export default function Dashboard() {
  return (
    <Suspense fallback={<div className="loader">Loading Dashboard...</div>}>
      <DashboardContent />
    </Suspense>
  );
}
