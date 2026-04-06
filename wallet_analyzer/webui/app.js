const state = {
  file: null,
  fileContent: "",
  latestResponse: null,
  latestDetails: null,
  downloadPrefix: "wallet-screen",
  activeJobId: null,
  pollToken: 0
};

const fileInput = document.getElementById("file-input");
const dropzone = document.getElementById("dropzone");
const fileMeta = document.getElementById("file-meta");
const analyzeForm = document.getElementById("analyze-form");
const analyzeButton = document.getElementById("analyze-button");
const statusText = document.getElementById("status-text");
const resultsPanel = document.getElementById("results-panel");
const summaryGrid = document.getElementById("summary-grid");
const spotlightGrid = document.getElementById("spotlight-grid");
const resultsBody = document.getElementById("results-body");
const invalidRowsBox = document.getElementById("invalid-rows");
const requestErrorsBox = document.getElementById("request-errors");
const serverStatus = document.getElementById("server-status");
const downloadCsvButton = document.getElementById("download-csv");
const downloadJsonButton = document.getElementById("download-json");
const downloadDetailsButton = document.getElementById("download-details");
const progressPanel = document.getElementById("progress-panel");
const progressPhase = document.getElementById("progress-phase");
const progressCount = document.getElementById("progress-count");
const progressFill = document.getElementById("progress-fill");
const progressMessage = document.getElementById("progress-message");

const controls = {
  apiKey: document.getElementById("api-key"),
  addressColumn: document.getElementById("address-column"),
  duration: document.getElementById("duration"),
  details: document.getElementById("details"),
  workers: document.getElementById("workers"),
  topTokens: document.getElementById("top-tokens"),
  retryPasses: document.getElementById("retry-passes"),
  minTotalTrades: document.getElementById("min-total-trades"),
  minUniqueTokens: document.getElementById("min-unique-tokens"),
  minTotalInvestedUsd: document.getElementById("min-total-invested-usd"),
  minWinRate: document.getElementById("min-win-rate"),
  minRealizedProfitUsd: document.getElementById("min-realized-profit-usd"),
  minTotalProfitUsd: document.getElementById("min-total-profit-usd")
};

initialize();

function initialize() {
  resetProgressPanel();
  setDownloadButtons(false);

  fileInput.addEventListener("change", async (event) => {
    const [file] = event.target.files;
    await loadFile(file);
  });

  ["dragenter", "dragover"].forEach((eventName) => {
    dropzone.addEventListener(eventName, (event) => {
      event.preventDefault();
      dropzone.classList.add("dragover");
    });
  });

  ["dragleave", "drop"].forEach((eventName) => {
    dropzone.addEventListener(eventName, (event) => {
      event.preventDefault();
      dropzone.classList.remove("dragover");
    });
  });

  dropzone.addEventListener("drop", async (event) => {
    const [file] = event.dataTransfer.files;
    await loadFile(file);
  });

  analyzeForm.addEventListener("submit", handleAnalyze);
  downloadCsvButton.addEventListener("click", () => {
    if (state.latestResponse?.csv_content) {
      downloadBlob(`${state.downloadPrefix}-screen.csv`, state.latestResponse.csv_content, "text/csv;charset=utf-8");
    }
  });
  downloadJsonButton.addEventListener("click", () => {
    if (state.latestResponse?.report) {
      downloadBlob(
        `${state.downloadPrefix}-screen.json`,
        JSON.stringify(state.latestResponse.report, null, 2),
        "application/json;charset=utf-8"
      );
    }
  });
  downloadDetailsButton.addEventListener("click", () => {
    if (state.latestDetails) {
      downloadBlob(
        `${state.downloadPrefix}-details.json`,
        JSON.stringify(state.latestDetails, null, 2),
        "application/json;charset=utf-8"
      );
    }
  });

  hydrateServerStatus();
}

async function hydrateServerStatus() {
  try {
    const response = await fetch("/api/health");
    const payload = await response.json();
    if (payload.has_default_api_key) {
      serverStatus.textContent = "Server is ready and already has a Birdeye API key configured.";
      serverStatus.className = "server-status good";
    } else {
      serverStatus.textContent = "Server is running, but no default Birdeye API key is configured yet. You can still paste one into the field above.";
      serverStatus.className = "server-status warn";
    }
  } catch (error) {
    serverStatus.textContent = "Could not reach the local API server. Make sure wallet-analyzer-web is running.";
    serverStatus.className = "server-status warn";
  }
}

async function loadFile(file) {
  if (!file) {
    return;
  }

  state.file = file;
  state.fileContent = await file.text();
  const sizeKb = Math.max(1, Math.round(file.size / 1024));
  fileMeta.textContent = `${file.name} loaded • ${sizeKb} KB`;
  statusText.textContent = "File loaded. Review your settings and start the analysis.";
  resetProgressPanel();
}

async function handleAnalyze(event) {
  event.preventDefault();

  if (!state.file || !state.fileContent.trim()) {
    statusText.textContent = "Choose a CSV, TXT, or JSON file first.";
    return;
  }

  const pollToken = state.pollToken + 1;
  state.pollToken = pollToken;
  state.activeJobId = null;
  state.latestResponse = null;
  state.latestDetails = null;

  setLoading(true, "Submitting the analysis job...");
  setDownloadButtons(false);
  updateProgressPanel(
    {
      phase: "queue",
      completed: 0,
      total: 0,
      message: "Uploading your file and preparing the analysis job.",
      progress_percent: 1
    },
    "queued"
  );

  const payload = {
    api_key: controls.apiKey.value.trim(),
    file_name: state.file.name,
    content: state.fileContent,
    address_column: controls.addressColumn.value.trim(),
    duration: controls.duration.value,
    details: controls.details.value,
    workers: Number(controls.workers.value || 4),
    top_tokens: Number(controls.topTokens.value || 3),
    retry_passes: Number(controls.retryPasses.value || 2),
    min_total_trades: Number(controls.minTotalTrades.value || 15),
    min_unique_tokens: Number(controls.minUniqueTokens.value || 3),
    min_total_invested_usd: Number(controls.minTotalInvestedUsd.value || 1000),
    min_win_rate: Number(controls.minWinRate.value || 0.5),
    min_realized_profit_usd: Number(controls.minRealizedProfitUsd.value || 250),
    min_total_profit_usd: Number(controls.minTotalProfitUsd.value || 500)
  };

  try {
    const response = await fetch("/api/analyze", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify(payload)
    });
    const data = await response.json();
    if (!response.ok || !data.success) {
      throw new Error(data.error || "Analysis failed.");
    }

    state.activeJobId = data.job_id;
    state.downloadPrefix = data.download_prefix || "wallet-screen";
    updateProgressPanel(
      {
        phase: "queue",
        completed: 0,
        total: 0,
        message: "Job accepted. Waiting for the analyzer to start.",
        progress_percent: 2
      },
      "queued"
    );

    const job = await pollJobUntilComplete(data.job_id, pollToken);
    if (pollToken !== state.pollToken) {
      return;
    }
    if (job.status !== "succeeded" || !job.result) {
      throw new Error(job.error || job.progress?.message || "Analysis failed.");
    }

    state.latestResponse = job.result;
    state.latestDetails = job.result.details || null;
    state.downloadPrefix = job.result.download_prefix || state.downloadPrefix;
    renderResults(job.result.report, job.result.details);
    updateProgressPanel(job.progress, job.status);
    statusText.textContent = "Analysis complete. You can inspect the table or download the reports.";
  } catch (error) {
    if (pollToken === state.pollToken) {
      const message = error instanceof Error ? error.message : "Analysis failed.";
      updateProgressPanel(
        {
          phase: "failed",
          completed: 0,
          total: 0,
          message,
          progress_percent: 100
        },
        "failed"
      );
      statusText.textContent = message;
    }
  } finally {
    if (pollToken === state.pollToken) {
      state.activeJobId = null;
      setLoading(false);
    }
  }
}

async function pollJobUntilComplete(jobId, pollToken) {
  while (pollToken === state.pollToken) {
    const response = await fetch(`/api/jobs/${encodeURIComponent(jobId)}`);
    const data = await response.json();
    if (!response.ok || !data.success) {
      throw new Error(data.error || "Could not fetch job progress.");
    }

    const job = data.job;
    updateProgressPanel(job.progress, job.status);

    if (job.status === "succeeded" || job.status === "failed") {
      return job;
    }

    await sleep(job.status === "queued" ? 450 : 800);
  }

  throw new Error("Analysis was interrupted.");
}

function renderResults(report, details) {
  resultsPanel.classList.remove("hidden");
  renderSummaryCards(report.summary, report.input);
  renderSpotlights(report.wallets || []);
  renderTable(report.wallets || []);
  renderMessageList(invalidRowsBox, report.invalid_rows, formatInvalidRow);
  renderMessageList(requestErrorsBox, report.request_errors, formatRequestError);
  setDownloadButtons(true);
}

function renderSummaryCards(summary, input) {
  const cards = [
    { label: "Screened", value: summary.screened_wallets, note: `${input.valid_wallets} valid wallets detected` },
    { label: "Profitable", value: summary.profitable, note: "Clear passes against your current thresholds" },
    { label: "Borderline", value: summary.borderline, note: "Positive signal, but still missing something" },
    { label: "Not profitable", value: summary.not_profitable, note: "Weak performance despite enough activity" },
    { label: "Insufficient history", value: summary.insufficient_history, note: "Too little capital or trade history" }
  ];

  summaryGrid.innerHTML = cards
    .map(
      (card) => `
        <article class="summary-card">
          <span class="metric-label">${escapeHtml(card.label)}</span>
          <strong>${escapeHtml(String(card.value))}</strong>
          <span>${escapeHtml(card.note)}</span>
        </article>
      `
    )
    .join("");
}

function renderSpotlights(rows) {
  const spotlightRows = rows.slice(0, 3);
  if (!spotlightRows.length) {
    spotlightGrid.innerHTML = `
      <article class="spotlight-card">
        <span class="metric-label">No successful wallet results</span>
        <strong>Only request errors came back.</strong>
        <span>Download the JSON report to inspect which wallets failed and why.</span>
      </article>
    `;
    return;
  }

  spotlightGrid.innerHTML = spotlightRows
    .map(
      (row, index) => `
        <article class="spotlight-card">
          <span class="metric-label">Rank ${index + 1}</span>
          <strong>${escapeHtml(shortWallet(row.wallet))}</strong>
          <span class="status-pill status-${escapeHtml(row.status)}">${escapeHtml(row.status.replaceAll("_", " "))}</span>
          <span>Score ${escapeHtml(String(row.score))} • Total PnL ${formatUsd(row.total_profit_usd)}</span>
          <span>${escapeHtml(row.status_reason)}</span>
        </article>
      `
    )
    .join("");
}

function renderTable(rows) {
  if (!rows.length) {
    resultsBody.innerHTML = `
      <tr>
        <td colspan="9">No successful wallet results were returned.</td>
      </tr>
    `;
    return;
  }

  resultsBody.innerHTML = rows
    .map(
      (row) => `
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
      `
    )
    .join("");
}

function renderMessageList(container, items, formatter) {
  if (!items || !items.length) {
    container.textContent = "None.";
    return;
  }
  container.textContent = items.map(formatter).join("\n");
}

function formatInvalidRow(item) {
  return `row ${item.source_row}: ${item.raw_value || "<empty>"} (${item.reason})`;
}

function formatRequestError(item) {
  return `${shortWallet(item.wallet)}: ${item.error}`;
}

function setLoading(isLoading, message = "") {
  analyzeButton.disabled = isLoading;
  analyzeButton.textContent = isLoading ? "Analyzing..." : "Analyze wallets";
  if (message) {
    statusText.textContent = message;
  }
}

function setDownloadButtons(enabled) {
  downloadCsvButton.disabled = !enabled || !state.latestResponse?.csv_content;
  downloadJsonButton.disabled = !enabled || !state.latestResponse?.report;
  downloadDetailsButton.disabled = !enabled || !state.latestDetails;
}

function resetProgressPanel() {
  progressPanel.classList.add("hidden");
  progressPanel.classList.remove("failed", "complete");
  progressPhase.textContent = "Waiting";
  progressCount.textContent = "0 / 0";
  progressFill.style.width = "0%";
  progressMessage.textContent = "No job running.";
}

function updateProgressPanel(progress = {}, jobStatus = "running") {
  const phase = progress.phase || "queue";
  const completed = Number(progress.completed || 0);
  const total = Number(progress.total || 0);
  const percent = clampProgress(progress.progress_percent);
  const message = progress.message || "Working through your wallets.";

  progressPanel.classList.remove("hidden");
  progressPanel.classList.toggle("failed", jobStatus === "failed" || phase === "failed");
  progressPanel.classList.toggle("complete", jobStatus === "succeeded" || phase === "done");
  progressPhase.textContent = formatPhaseLabel(phase, jobStatus);
  progressCount.textContent = formatProgressCount(phase, completed, total);
  progressFill.style.width = `${percent}%`;
  progressMessage.textContent = message;

  if (jobStatus === "queued" || jobStatus === "running") {
    statusText.textContent = message;
  }
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
    prepare: "Preparing wallets",
    screening: "Screening wallets",
    details: "Loading token details"
  };
  return labels[phase] || "Running";
}

function formatProgressCount(phase, completed, total) {
  if (total > 0) {
    return `${completed} / ${total}`;
  }
  if (phase === "queue") {
    return "starting";
  }
  return `${completed}`;
}

function clampProgress(value) {
  const number = Number(value);
  if (!Number.isFinite(number)) {
    return 0;
  }
  return Math.max(0, Math.min(100, Math.round(number)));
}

function sleep(ms) {
  return new Promise((resolve) => {
    window.setTimeout(resolve, ms);
  });
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
    return wallet;
  }
  return `${wallet.slice(0, 6)}...${wallet.slice(-4)}`;
}

function formatUsd(value) {
  const number = Number(value || 0);
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 0
  }).format(number);
}

function formatPercent(value) {
  const number = Number(value || 0);
  return `${number.toFixed(1)}%`;
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}
