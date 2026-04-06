const state = {
  activeTab: "wallet",
  walletFile: null,
  walletFileContent: "",
  walletResponse: null,
  walletDetails: null,
  walletDownloadPrefix: "wallet-screen",
  tokenResponse: null,
  tokenDownloadPrefix: "token-intel",
  pollToken: 0
};

const elements = {
  sharedApiKey: document.getElementById("shared-api-key"),
  serverStatus: document.getElementById("server-status"),
  tabButtons: document.querySelectorAll(".tab-button"),
  walletPanel: document.getElementById("panel-wallet"),
  tokenPanel: document.getElementById("panel-token"),
  fileInput: document.getElementById("file-input"),
  dropzone: document.getElementById("dropzone"),
  fileMeta: document.getElementById("file-meta"),
  walletForm: document.getElementById("wallet-form"),
  tokenForm: document.getElementById("token-form"),
  walletAnalyzeButton: document.getElementById("wallet-analyze-button"),
  tokenAnalyzeButton: document.getElementById("token-analyze-button"),
  walletStatusText: document.getElementById("wallet-status-text"),
  tokenStatusText: document.getElementById("token-status-text"),
  walletResultsPanel: document.getElementById("wallet-results-panel"),
  tokenResultsPanel: document.getElementById("token-results-panel"),
  summaryGrid: document.getElementById("summary-grid"),
  spotlightGrid: document.getElementById("spotlight-grid"),
  resultsBody: document.getElementById("results-body"),
  invalidRowsBox: document.getElementById("invalid-rows"),
  requestErrorsBox: document.getElementById("request-errors"),
  downloadCsvButton: document.getElementById("download-csv"),
  downloadJsonButton: document.getElementById("download-json"),
  downloadDetailsButton: document.getElementById("download-details"),
  walletProgressPanel: document.getElementById("wallet-progress-panel"),
  walletProgressPhase: document.getElementById("wallet-progress-phase"),
  walletProgressCount: document.getElementById("wallet-progress-count"),
  walletProgressFill: document.getElementById("wallet-progress-fill"),
  walletProgressMessage: document.getElementById("wallet-progress-message"),
  tokenProgressPanel: document.getElementById("token-progress-panel"),
  tokenProgressPhase: document.getElementById("token-progress-phase"),
  tokenProgressCount: document.getElementById("token-progress-count"),
  tokenProgressFill: document.getElementById("token-progress-fill"),
  tokenProgressMessage: document.getElementById("token-progress-message"),
  tokenSummaryGrid: document.getElementById("token-summary-grid"),
  tokenClustersGrid: document.getElementById("token-clusters-grid"),
  tokenResultsBody: document.getElementById("token-results-body"),
  tokenHoldersBox: document.getElementById("token-holders-box"),
  tokenNotesBox: document.getElementById("token-notes-box"),
  downloadTokenCsvButton: document.getElementById("download-token-csv"),
  downloadTokenJsonButton: document.getElementById("download-token-json")
};

const walletControls = {
  addressColumn: document.getElementById("address-column"),
  duration: document.getElementById("duration"),
  details: document.getElementById("details"),
  workers: document.getElementById("workers"),
  topTokens: document.getElementById("top-tokens"),
  retryPasses: document.getElementById("retry-passes"),
  timeout: document.getElementById("wallet-timeout"),
  minTotalTrades: document.getElementById("min-total-trades"),
  minUniqueTokens: document.getElementById("min-unique-tokens"),
  minTotalInvestedUsd: document.getElementById("min-total-invested-usd"),
  minWinRate: document.getElementById("min-win-rate"),
  minRealizedProfitUsd: document.getElementById("min-realized-profit-usd"),
  minTotalProfitUsd: document.getElementById("min-total-profit-usd")
};

const tokenControls = {
  tokenAddress: document.getElementById("token-address"),
  profitabilityDuration: document.getElementById("token-duration"),
  holderLimit: document.getElementById("holder-limit"),
  tradeLimit: document.getElementById("trade-limit"),
  earlyBuyerLimit: document.getElementById("early-buyer-limit"),
  traderLimit: document.getElementById("trader-limit"),
  candidateLimit: document.getElementById("candidate-limit")
};

initialize();

function initialize() {
  resetProgress(elements.walletProgressPanel, elements.walletProgressPhase, elements.walletProgressCount, elements.walletProgressFill, elements.walletProgressMessage);
  resetProgress(elements.tokenProgressPanel, elements.tokenProgressPhase, elements.tokenProgressCount, elements.tokenProgressFill, elements.tokenProgressMessage);
  setWalletDownloads(false);
  setTokenDownloads(false);

  elements.tabButtons.forEach((button) => {
    button.addEventListener("click", () => setActiveTab(button.dataset.tab));
  });

  elements.fileInput.addEventListener("change", async (event) => {
    const [file] = event.target.files;
    await loadWalletFile(file);
  });

  ["dragenter", "dragover"].forEach((eventName) => {
    elements.dropzone.addEventListener(eventName, (event) => {
      event.preventDefault();
      elements.dropzone.classList.add("dragover");
    });
  });

  ["dragleave", "drop"].forEach((eventName) => {
    elements.dropzone.addEventListener(eventName, (event) => {
      event.preventDefault();
      elements.dropzone.classList.remove("dragover");
    });
  });

  elements.dropzone.addEventListener("drop", async (event) => {
    const [file] = event.dataTransfer.files;
    await loadWalletFile(file);
  });

  elements.walletForm.addEventListener("submit", handleWalletAnalyze);
  elements.tokenForm.addEventListener("submit", handleTokenAnalyze);

  elements.downloadCsvButton.addEventListener("click", () => {
    if (state.walletResponse?.csv_content) {
      downloadBlob(`${state.walletDownloadPrefix}-screen.csv`, state.walletResponse.csv_content, "text/csv;charset=utf-8");
    }
  });
  elements.downloadJsonButton.addEventListener("click", () => {
    if (state.walletResponse?.report) {
      downloadBlob(`${state.walletDownloadPrefix}-screen.json`, JSON.stringify(state.walletResponse.report, null, 2), "application/json;charset=utf-8");
    }
  });
  elements.downloadDetailsButton.addEventListener("click", () => {
    if (state.walletDetails) {
      downloadBlob(`${state.walletDownloadPrefix}-details.json`, JSON.stringify(state.walletDetails, null, 2), "application/json;charset=utf-8");
    }
  });
  elements.downloadTokenCsvButton.addEventListener("click", () => {
    if (state.tokenResponse?.csv_content) {
      downloadBlob(`${state.tokenDownloadPrefix}.csv`, state.tokenResponse.csv_content, "text/csv;charset=utf-8");
    }
  });
  elements.downloadTokenJsonButton.addEventListener("click", () => {
    if (state.tokenResponse?.report) {
      downloadBlob(`${state.tokenDownloadPrefix}.json`, JSON.stringify(state.tokenResponse.report, null, 2), "application/json;charset=utf-8");
    }
  });

  hydrateServerStatus();
}

function setActiveTab(tab) {
  state.activeTab = tab;
  elements.tabButtons.forEach((button) => {
    const isActive = button.dataset.tab === tab;
    button.classList.toggle("active", isActive);
    button.setAttribute("aria-selected", String(isActive));
  });
  elements.walletPanel.classList.toggle("hidden", tab !== "wallet");
  elements.tokenPanel.classList.toggle("hidden", tab !== "token");
}

async function hydrateServerStatus() {
  try {
    const response = await fetch("/api/health");
    const payload = await response.json();
    if (payload.has_default_api_key) {
      elements.serverStatus.textContent = "Server is ready and already has a Birdeye API key configured.";
      elements.serverStatus.className = "server-status good";
    } else {
      elements.serverStatus.textContent = "Server is running, but no default Birdeye API key is configured yet. You can still paste one into the field above.";
      elements.serverStatus.className = "server-status warn";
    }
  } catch (error) {
    elements.serverStatus.textContent = "Could not reach the local API server. Make sure wallet-analyzer-web is running.";
    elements.serverStatus.className = "server-status warn";
  }
}

async function loadWalletFile(file) {
  if (!file) {
    return;
  }
  state.walletFile = file;
  state.walletFileContent = await file.text();
  const sizeKb = Math.max(1, Math.round(file.size / 1024));
  elements.fileMeta.textContent = `${file.name} loaded • ${sizeKb} KB`;
  elements.walletStatusText.textContent = "File loaded. Review your settings and start the analysis.";
  resetProgress(elements.walletProgressPanel, elements.walletProgressPhase, elements.walletProgressCount, elements.walletProgressFill, elements.walletProgressMessage);
}

async function handleWalletAnalyze(event) {
  event.preventDefault();
  if (!state.walletFile || !state.walletFileContent.trim()) {
    elements.walletStatusText.textContent = "Choose a CSV, TXT, or JSON file first.";
    return;
  }

  const token = nextPollToken();
  state.walletResponse = null;
  state.walletDetails = null;
  setWalletLoading(true, "Submitting the wallet analysis job...");
  setWalletDownloads(false);
  updateProgress(elements.walletProgressPanel, elements.walletProgressPhase, elements.walletProgressCount, elements.walletProgressFill, elements.walletProgressMessage, {
    phase: "queue", completed: 0, total: 0, message: "Uploading your wallet file and preparing the job.", progress_percent: 1
  }, "queued", elements.walletStatusText);

  const payload = {
    api_key: elements.sharedApiKey.value.trim(),
    file_name: state.walletFile.name,
    content: state.walletFileContent,
    address_column: walletControls.addressColumn.value.trim(),
    duration: walletControls.duration.value,
    details: walletControls.details.value,
    workers: Number(walletControls.workers.value || 4),
    top_tokens: Number(walletControls.topTokens.value || 3),
    retry_passes: Number(walletControls.retryPasses.value || 2),
    timeout: Number(walletControls.timeout.value || 20),
    min_total_trades: Number(walletControls.minTotalTrades.value || 15),
    min_unique_tokens: Number(walletControls.minUniqueTokens.value || 3),
    min_total_invested_usd: Number(walletControls.minTotalInvestedUsd.value || 1000),
    min_win_rate: Number(walletControls.minWinRate.value || 0.5),
    min_realized_profit_usd: Number(walletControls.minRealizedProfitUsd.value || 250),
    min_total_profit_usd: Number(walletControls.minTotalProfitUsd.value || 500)
  };

  try {
    const job = await submitAndPollJob("/api/analyze", payload, token, {
      panel: elements.walletProgressPanel,
      phase: elements.walletProgressPhase,
      count: elements.walletProgressCount,
      fill: elements.walletProgressFill,
      message: elements.walletProgressMessage,
      statusText: elements.walletStatusText
    });
    state.walletResponse = job.result;
    state.walletDetails = job.result.details || null;
    state.walletDownloadPrefix = job.result.download_prefix || "wallet-screen";
    renderWalletResults(job.result.report, job.result.details);
    elements.walletStatusText.textContent = "Wallet analysis complete. You can inspect the table or download the reports.";
  } catch (error) {
    if (token === state.pollToken) {
      elements.walletStatusText.textContent = error.message;
    }
  } finally {
    if (token === state.pollToken) {
      setWalletLoading(false);
    }
  }
}

async function handleTokenAnalyze(event) {
  event.preventDefault();
  if (!tokenControls.tokenAddress.value.trim()) {
    elements.tokenStatusText.textContent = "Enter a token contract address first.";
    return;
  }

  const token = nextPollToken();
  state.tokenResponse = null;
  setTokenLoading(true, "Submitting the token intel job...");
  setTokenDownloads(false);
  updateProgress(elements.tokenProgressPanel, elements.tokenProgressPhase, elements.tokenProgressCount, elements.tokenProgressFill, elements.tokenProgressMessage, {
    phase: "queue", completed: 0, total: 0, message: "Submitting token intel request.", progress_percent: 1
  }, "queued", elements.tokenStatusText);

  const payload = {
    api_key: elements.sharedApiKey.value.trim(),
    token_address: tokenControls.tokenAddress.value.trim(),
    profitability_duration: tokenControls.profitabilityDuration.value,
    holder_limit: Number(tokenControls.holderLimit.value || 30),
    trade_limit: Number(tokenControls.tradeLimit.value || 200),
    early_buyer_limit: Number(tokenControls.earlyBuyerLimit.value || 20),
    trader_limit: Number(tokenControls.traderLimit.value || 20),
    candidate_limit: Number(tokenControls.candidateLimit.value || 40)
  };

  try {
    const job = await submitAndPollJob("/api/token-intel", payload, token, {
      panel: elements.tokenProgressPanel,
      phase: elements.tokenProgressPhase,
      count: elements.tokenProgressCount,
      fill: elements.tokenProgressFill,
      message: elements.tokenProgressMessage,
      statusText: elements.tokenStatusText
    });
    state.tokenResponse = job.result;
    state.tokenDownloadPrefix = job.result.download_prefix || "token-intel";
    renderTokenResults(job.result.report);
    elements.tokenStatusText.textContent = "Token intel complete. Review the candidate wallets and download the report if needed.";
  } catch (error) {
    if (token === state.pollToken) {
      elements.tokenStatusText.textContent = error.message;
    }
  } finally {
    if (token === state.pollToken) {
      setTokenLoading(false);
    }
  }
}

async function submitAndPollJob(endpoint, payload, token, progressEls) {
  const response = await fetch(endpoint, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  const data = await response.json();
  if (!response.ok || !data.success) {
    throw new Error(data.error || "Analysis failed.");
  }

  updateProgress(progressEls.panel, progressEls.phase, progressEls.count, progressEls.fill, progressEls.message, {
    phase: "queue", completed: 0, total: 0, message: "Job accepted. Waiting for the analyzer to start.", progress_percent: 2
  }, "queued", progressEls.statusText);

  while (token === state.pollToken) {
    const jobResponse = await fetch(`/api/jobs/${encodeURIComponent(data.job_id)}`);
    const jobData = await jobResponse.json();
    if (!jobResponse.ok || !jobData.success) {
      throw new Error(jobData.error || "Could not fetch job progress.");
    }
    const job = jobData.job;
    updateProgress(progressEls.panel, progressEls.phase, progressEls.count, progressEls.fill, progressEls.message, job.progress, job.status, progressEls.statusText);
    if (job.status === "succeeded") {
      return job;
    }
    if (job.status === "failed") {
      throw new Error(job.error || job.progress?.message || "Analysis failed.");
    }
    await sleep(job.status === "queued" ? 450 : 800);
  }

  throw new Error("Analysis was interrupted.");
}

function renderWalletResults(report, details) {
  elements.walletResultsPanel.classList.remove("hidden");
  renderSummaryCards(report.summary, report.input);
  renderWalletSpotlights(report.wallets || []);
  renderWalletTable(report.wallets || []);
  renderMessageList(elements.invalidRowsBox, report.invalid_rows, formatInvalidRow);
  renderMessageList(elements.requestErrorsBox, report.request_errors, formatRequestError);
  setWalletDownloads(true);
}

function renderSummaryCards(summary, input) {
  const cards = [
    { label: "Screened", value: summary.screened_wallets, note: `${input.valid_wallets} valid wallets detected` },
    { label: "Profitable", value: summary.profitable, note: "Clear passes against your current thresholds" },
    { label: "Borderline", value: summary.borderline, note: "Positive signal, but still missing something" },
    { label: "Not profitable", value: summary.not_profitable, note: "Weak performance despite enough activity" },
    { label: "Insufficient history", value: summary.insufficient_history, note: "Too little capital or trade history" }
  ];

  elements.summaryGrid.innerHTML = cards.map((card) => `
    <article class="summary-card">
      <span class="metric-label">${escapeHtml(card.label)}</span>
      <strong>${escapeHtml(String(card.value))}</strong>
      <span>${escapeHtml(card.note)}</span>
    </article>
  `).join("");
}

function renderWalletSpotlights(rows) {
  const spotlightRows = rows.slice(0, 3);
  if (!spotlightRows.length) {
    elements.spotlightGrid.innerHTML = `
      <article class="spotlight-card">
        <span class="metric-label">No successful wallet results</span>
        <strong>Only request errors came back.</strong>
        <span>Download the JSON report to inspect which wallets failed and why.</span>
      </article>
    `;
    return;
  }
  elements.spotlightGrid.innerHTML = spotlightRows.map((row, index) => `
    <article class="spotlight-card">
      <span class="metric-label">Rank ${index + 1}</span>
      <strong>${escapeHtml(shortWallet(row.wallet))}</strong>
      <span class="status-pill status-${escapeHtml(row.status)}">${escapeHtml(row.status.replaceAll("_", " "))}</span>
      <span>Score ${escapeHtml(String(row.score))} • Total PnL ${formatUsd(row.total_profit_usd)}</span>
      <span>${escapeHtml(row.status_reason)}</span>
    </article>
  `).join("");
}

function renderWalletTable(rows) {
  if (!rows.length) {
    elements.resultsBody.innerHTML = `<tr><td colspan="9">No successful wallet results were returned.</td></tr>`;
    return;
  }
  elements.resultsBody.innerHTML = rows.map((row) => `
    <tr>
      <td class="wallet-cell">${escapeHtml(row.wallet)}</td>
      <td><span class="status-pill status-${escapeHtml(row.status)}">${escapeHtml(row.status.replaceAll("_", " "))}</span></td>
      <td class="numeric">${escapeHtml(String(row.score))}</td>
      <td class="numeric">${formatPercent(row.win_rate_pct)}</td>
      <td class="numeric">${formatUsd(row.total_invested_usd)}</td>
      <td class="numeric">${formatUsd(row.realized_profit_usd)}</td>
      <td class="numeric">${formatUsd(row.total_profit_usd)}</td>
      <td class="numeric">${formatPercent(row.estimated_total_roi_pct)}</td>
      <td>${escapeHtml(row.top_tokens || "-")}</td>
    </tr>
  `).join("");
}

function renderTokenResults(report) {
  elements.tokenResultsPanel.classList.remove("hidden");
  renderTokenSummary(report);
  renderTokenClusters(report.clusters || []);
  renderTokenTable(report.candidates || []);
  renderTokenHolders(report.top_holders || []);
  renderMessageList(elements.tokenNotesBox, report.analysis_notes, (item) => item);
  setTokenDownloads(true);
}

function renderTokenSummary(report) {
  const token = report.token || {};
  const summary = report.summary || {};
  const cards = [
    { label: token.symbol || "Token", value: token.name || shortWallet(token.address || ""), note: token.address || "" },
    { label: "Candidates", value: summary.candidate_wallets || 0, note: `${summary.profitable_wallets || 0} profitable overall wallets` },
    { label: "Clusters", value: summary.clusters_found || 0, note: `${summary.borderline_wallets || 0} borderline overall wallets` },
    { label: "Price", value: formatUsd(token.price_usd), note: `MC ${formatUsd(token.market_cap)}` },
    { label: "Liquidity", value: formatUsd(token.liquidity_usd), note: `${token.holders || 0} holders reported` }
  ];
  elements.tokenSummaryGrid.innerHTML = cards.map((card) => `
    <article class="summary-card">
      <span class="metric-label">${escapeHtml(card.label)}</span>
      <strong>${escapeHtml(String(card.value))}</strong>
      <span>${escapeHtml(card.note)}</span>
    </article>
  `).join("");
}

function renderTokenClusters(clusters) {
  if (!clusters.length) {
    elements.tokenClustersGrid.innerHTML = `
      <article class="spotlight-card">
        <span class="metric-label">No strong funding clusters</span>
        <strong>Each candidate wallet appears to have a distinct first funding source.</strong>
        <span>That can still be useful if the highest-ranked wallets are independently strong.</span>
      </article>
    `;
    return;
  }
  elements.tokenClustersGrid.innerHTML = clusters.slice(0, 3).map((cluster, index) => `
    <article class="spotlight-card">
      <span class="metric-label">Cluster ${index + 1}</span>
      <strong>${escapeHtml(shortWallet(cluster.funding_source))}</strong>
      <span>${escapeHtml(String(cluster.wallet_count))} candidate wallets share this first funding source.</span>
      <span>${escapeHtml(cluster.wallets.slice(0, 3).map(shortWallet).join(", "))}</span>
    </article>
  `).join("");
}

function renderTokenTable(rows) {
  if (!rows.length) {
    elements.tokenResultsBody.innerHTML = `<tr><td colspan="9">No candidate wallets were returned for this token.</td></tr>`;
    return;
  }
  elements.tokenResultsBody.innerHTML = rows.map((row) => `
    <tr>
      <td class="wallet-cell">${escapeHtml(row.wallet)}</td>
      <td class="numeric">${escapeHtml(String(row.alpha_score ?? "-"))}</td>
      <td>${escapeHtml((row.source_tags || []).join(", "))}</td>
      <td>${row.wallet_status ? `<span class="status-pill status-${escapeHtml(row.wallet_status)}">${escapeHtml(row.wallet_status.replaceAll("_", " "))}</span>` : "-"}</td>
      <td class="numeric">${formatUsd(row.holder_value_usd)}</td>
      <td class="numeric">${formatUsd(row.trade_volume_usd)}</td>
      <td class="numeric">${row.early_rank ?? "-"}</td>
      <td>${row.funding_cluster_size ? `${escapeHtml(String(row.funding_cluster_size))} via ${escapeHtml(shortWallet(row.funding_source || ""))}` : "-"}</td>
      <td>${escapeHtml((row.notes || []).join(" | "))}</td>
    </tr>
  `).join("");
}

function renderTokenHolders(rows) {
  if (!rows.length) {
    elements.tokenHoldersBox.textContent = "None.";
    return;
  }
  elements.tokenHoldersBox.textContent = rows.map((row, index) => `${index + 1}. ${shortWallet(row.wallet)} | ${formatUsd(row.value_usd)} | ${Number(row.share_pct || 0).toFixed(2)}%`).join("\n");
}

function renderMessageList(container, items, formatter) {
  if (!items || !items.length) {
    container.textContent = "None.";
    return;
  }
  container.textContent = items.map(formatter).join("\n");
}

function setWalletLoading(isLoading, message = "") {
  elements.walletAnalyzeButton.disabled = isLoading;
  elements.walletAnalyzeButton.textContent = isLoading ? "Analyzing..." : "Analyze wallets";
  if (message) {
    elements.walletStatusText.textContent = message;
  }
}

function setTokenLoading(isLoading, message = "") {
  elements.tokenAnalyzeButton.disabled = isLoading;
  elements.tokenAnalyzeButton.textContent = isLoading ? "Analyzing..." : "Analyze token";
  if (message) {
    elements.tokenStatusText.textContent = message;
  }
}

function setWalletDownloads(enabled) {
  elements.downloadCsvButton.disabled = !enabled || !state.walletResponse?.csv_content;
  elements.downloadJsonButton.disabled = !enabled || !state.walletResponse?.report;
  elements.downloadDetailsButton.disabled = !enabled || !state.walletDetails;
}

function setTokenDownloads(enabled) {
  elements.downloadTokenCsvButton.disabled = !enabled || !state.tokenResponse?.csv_content;
  elements.downloadTokenJsonButton.disabled = !enabled || !state.tokenResponse?.report;
}

function resetProgress(panel, phase, count, fill, message) {
  panel.classList.add("hidden");
  panel.classList.remove("failed", "complete");
  phase.textContent = "Waiting";
  count.textContent = "0 / 0";
  fill.style.width = "0%";
  message.textContent = "No job running.";
}

function updateProgress(panel, phaseEl, countEl, fillEl, messageEl, progress = {}, jobStatus = "running", statusTextEl = null) {
  const phase = progress.phase || "queue";
  const completed = Number(progress.completed || 0);
  const total = Number(progress.total || 0);
  const percent = clampProgress(progress.progress_percent);
  const message = progress.message || "Working through the request.";

  panel.classList.remove("hidden");
  panel.classList.toggle("failed", jobStatus === "failed" || phase === "failed");
  panel.classList.toggle("complete", jobStatus === "succeeded" || phase === "done");
  phaseEl.textContent = formatPhaseLabel(phase, jobStatus);
  countEl.textContent = formatProgressCount(phase, completed, total);
  fillEl.style.width = `${percent}%`;
  messageEl.textContent = message;
  if (statusTextEl && (jobStatus === "queued" || jobStatus === "running")) {
    statusTextEl.textContent = message;
  }
}

function nextPollToken() {
  state.pollToken += 1;
  return state.pollToken;
}

function formatPhaseLabel(phase, jobStatus) {
  if (jobStatus === "failed" || phase === "failed") {
    return "Run failed";
  }
  if (jobStatus === "succeeded" || phase === "done") {
    return "Run complete";
  }
  const labels = {
    queue: "Queued",
    prepare: "Preparing data",
    screening: "Screening wallets",
    details: "Loading details"
  };
  return labels[phase] || "Running";
}

function formatProgressCount(phase, completed, total) {
  if (total > 0) {
    return `${completed} / ${total}`;
  }
  return phase === "queue" ? "starting" : `${completed}`;
}

function formatInvalidRow(item) {
  return `row ${item.source_row}: ${item.raw_value || "<empty>"} (${item.reason})`;
}

function formatRequestError(item) {
  return `${shortWallet(item.wallet)}: ${item.error}`;
}

function downloadBlob(filename, content, type) {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement("a");
  anchor.href = url;
  anchor.download = filename;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function shortWallet(wallet) {
  if (!wallet || wallet.length < 12) {
    return wallet || "-";
  }
  return `${wallet.slice(0, 6)}...${wallet.slice(-4)}`;
}

function formatUsd(value) {
  const number = Number(value || 0);
  return new Intl.NumberFormat("en-US", { style: "currency", currency: "USD", maximumFractionDigits: 0 }).format(number);
}

function formatPercent(value) {
  const number = Number(value || 0);
  return `${number.toFixed(1)}%`;
}

function clampProgress(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return 0;
  }
  return Math.max(0, Math.min(100, Math.round(number)));
}

function sleep(ms) {
  return new Promise((resolve) => window.setTimeout(resolve, ms));
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
