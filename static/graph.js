let cyInstance = null;
let latestAnalysis = null;
let recentRows = [];

const CLIENT_TIMEOUT_MS = 30000;
const MAX_UPLOAD_BYTES = 10 * 1024 * 1024;
const MAX_GRAPH_NODES = 420;
const MAX_GRAPH_EDGES = 2200;
const TX_PAGE_SIZE = 15;

function setText(el, value) {
    if (!el) return;
    el.textContent = value;
}

function numberFmt(value, digits = 0) {
    const n = Number(value || 0);
    return n.toLocaleString(undefined, {
        minimumFractionDigits: digits,
        maximumFractionDigits: digits,
    });
}

function animateMetric(el, target, suffix = "") {
    if (!el) return;
    const duration = 500;
    const start = performance.now();
    const to = Number(target) || 0;

    function frame(now) {
        const t = Math.min(1, (now - start) / duration);
        const eased = t * (2 - t);
        const value = to * eased;
        el.textContent = `${numberFmt(value, suffix ? 2 : 0)}${suffix}`;
        if (t < 1) requestAnimationFrame(frame);
    }

    requestAnimationFrame(frame);
}

function scoreColor(score) {
    const s = Number(score || 0);
    if (s >= 90) return "#ef4444";
    if (s >= 70) return "#f97316";
    if (s >= 40) return "#8b5cf6";
    return "#22c55e";
}

function updateSummaryCards(summary) {
    if (!summary) return;
    animateMetric(document.querySelector("#metric-total-accounts .metric-value"), summary.total_accounts);
    animateMetric(document.querySelector("#metric-suspicious-accounts .metric-value"), summary.suspicious_accounts);
    animateMetric(document.querySelector("#metric-fraud-rings .metric-value"), summary.fraud_rings);
    animateMetric(document.querySelector("#metric-processing-time .metric-value"), summary.processing_time_seconds || 0, "s");
}

function computeGraphStats(nodes, edges) {
    const nodeCount = nodes.length;
    const edgeCount = edges.length;
    const avgDegree = nodeCount ? (2 * edgeCount) / nodeCount : 0;

    const parent = new Map();
    const find = (x) => {
        let p = parent.get(x);
        while (p !== parent.get(p)) {
            p = parent.get(p);
        }
        return p;
    };
    const union = (a, b) => {
        const ra = find(a);
        const rb = find(b);
        if (ra !== rb) parent.set(rb, ra);
    };

    nodes.forEach((n) => parent.set(n.account_id, n.account_id));
    edges.forEach((e) => {
        if (parent.has(e.source) && parent.has(e.target)) {
            union(e.source, e.target);
        }
    });

    const counts = new Map();
    nodes.forEach((n) => {
        const r = find(n.account_id);
        counts.set(r, (counts.get(r) || 0) + 1);
    });

    const largest = Math.max(0, ...counts.values());
    return { nodeCount, edgeCount, avgDegree, largest };
}

function renderGraphStats(nodes, edges) {
    const stats = computeGraphStats(nodes, edges);
    setText(document.getElementById("stat-nodes"), numberFmt(stats.nodeCount));
    setText(document.getElementById("stat-edges"), numberFmt(stats.edgeCount));
    setText(document.getElementById("stat-degree"), numberFmt(stats.avgDegree, 1));
    setText(document.getElementById("stat-component"), numberFmt(stats.largest));
}

function buildGraph(data) {
    const container = document.getElementById("graph-container");
    if (!container) return;

    const allNodes = data.nodes || [];
    const allEdges = data.edges || [];

    renderGraphStats(allNodes, allEdges);

    const selectedNodes = allNodes
        .slice()
        .sort((a, b) => Number(b.suspicion_score || 0) - Number(a.suspicion_score || 0))
        .slice(0, MAX_GRAPH_NODES);

    const selectedNodeIds = new Set(selectedNodes.map((n) => n.account_id));
    const selectedEdges = allEdges
        .filter((e) => selectedNodeIds.has(e.source) && selectedNodeIds.has(e.target))
        .sort((a, b) => Number(b.transaction_count || 0) - Number(a.transaction_count || 0))
        .slice(0, MAX_GRAPH_EDGES);

    const elements = [];

    selectedNodes.forEach((node) => {
        elements.push({
            data: {
                id: node.account_id,
                label: node.account_id,
                suspicion_score: Number(node.suspicion_score || 0),
                is_suspicious: !!node.is_suspicious,
                ring_id: node.ring_id || "",
                patterns: (node.patterns || []).join(","),
            },
        });
    });

    selectedEdges.forEach((edge, i) => {
        elements.push({
            data: {
                id: `${edge.source}_${edge.target}_${i}`,
                source: edge.source,
                target: edge.target,
                transaction_count: Number(edge.transaction_count || 0),
                total_amount: Number(edge.total_amount || 0),
            },
        });
    });

    if (cyInstance) cyInstance.destroy();

    const largeGraph = selectedNodes.length > 260;

    cyInstance = cytoscape({
        container,
        elements,
        style: [
            {
                selector: "node",
                style: {
                    label: "",
                    color: "#d8eaff",
                    "font-size": "8px",
                    "text-valign": "bottom",
                    "text-margin-y": 6,
                    "background-color": "#22c55e",
                    width: "mapData(suspicion_score, 0, 100, 10, 30)",
                    height: "mapData(suspicion_score, 0, 100, 10, 30)",
                    "border-width": 1,
                    "border-color": "#9ab3d3",
                },
            },
            { selector: "node[suspicion_score >= 40][suspicion_score < 70]", style: { "background-color": "#8b5cf6" } },
            { selector: "node[suspicion_score >= 70][suspicion_score < 90]", style: { "background-color": "#f97316", label: "data(label)" } },
            { selector: "node[suspicion_score >= 90]", style: { "background-color": "#ef4444", label: "data(label)" } },
            { selector: "node[ring_id != '']", style: { "border-width": 2, "border-color": "#f59e0b" } },
            {
                selector: "edge",
                style: {
                    width: "mapData(transaction_count, 1, 20, 0.8, 3.6)",
                    "line-color": "#6f90ba",
                    opacity: 0.45,
                    "curve-style": "bezier",
                    "target-arrow-shape": "triangle",
                    "target-arrow-color": "#6f90ba",
                    "arrow-scale": 0.6,
                },
            },
            {
                selector: "edge[transaction_count >= 8]",
                style: {
                    opacity: 0.85,
                    "line-color": "#2b8cff",
                    "target-arrow-color": "#2b8cff",
                },
            },
        ],
        layout: {
            name: "cose",
            idealEdgeLength: largeGraph ? 70 : 100,
            nodeRepulsion: largeGraph ? 3800 : 6200,
            gravity: 0.7,
            animate: !largeGraph,
            numIter: largeGraph ? 350 : 800,
        },
        wheelSensitivity: 0.18,
    });

    const tooltip = document.getElementById("graph-tooltip");

    function showTooltip(node, event) {
        if (!tooltip) return;
        const d = node.data();
        const patterns = (d.patterns || "").split(",").filter(Boolean).join(", ") || "None";

        tooltip.innerHTML = `
            <div class="tooltip-title">${d.id}</div>
            <div class="tooltip-row"><span>Suspicion</span><span>${Number(d.suspicion_score).toFixed(1)}</span></div>
            <div class="tooltip-row"><span>Ring</span><span>${d.ring_id || "None"}</span></div>
            <div class="tooltip-row"><span>Patterns</span><span>${patterns}</span></div>
        `;

        const p = event.renderedPosition || { x: 0, y: 0 };
        tooltip.style.left = `${p.x + 14}px`;
        tooltip.style.top = `${p.y + 14}px`;
        tooltip.hidden = false;
    }

    function hideTooltip() {
        if (tooltip) tooltip.hidden = true;
    }

    cyInstance.on("mouseover", "node", (evt) => showTooltip(evt.target, evt));
    cyInstance.on("mouseout", "node", hideTooltip);
    cyInstance.on("zoom pan drag", hideTooltip);
    cyInstance.on("tap", "node", (evt) => {
        const id = evt.target.id();
        if (!latestAnalysis) return;
        const acc = (latestAnalysis.nodes || []).find((n) => n.account_id === id);
        if (acc) updateAccountDetails(acc);
    });
}

function riskChip(score) {
    const s = Number(score || 0);
    if (s >= 80) return '<span class="risk-chip risk-high">High</span>';
    if (s >= 55) return '<span class="risk-chip risk-med">Medium</span>';
    return '<span class="risk-chip risk-low">Low</span>';
}

function renderFraudRingsTable(rings) {
    const tbody = document.querySelector("#fraud-rings-table tbody");
    if (!tbody) return;
    tbody.innerHTML = "";

    if (!rings || rings.length === 0) {
        tbody.innerHTML = "<tr class='placeholder-row'><td colspan='5'>No fraud rings detected.</td></tr>";
        return;
    }

    rings
        .slice()
        .sort((a, b) => Number(b.risk_score || 0) - Number(a.risk_score || 0))
        .forEach((ring) => {
            const risk = Number(ring.risk_score || 0);
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td>${ring.ring_id || ""}</td>
                <td>${ring.pattern_type || "cycle"}</td>
                <td>${ring.member_count || 0}</td>
                <td>${riskChip(risk)} ${risk.toFixed(0)}</td>
                <td class="mono">${(ring.member_accounts || []).join(", ")}</td>
            `;
            tbody.appendChild(tr);
        });
}

function renderScoreDistribution(nodes) {
    const host = document.getElementById("score-distribution");
    if (!host) return;
    host.innerHTML = "";

    const bins = [
        { label: "0-39", min: 0, max: 39, color: "#22c55e" },
        { label: "40-69", min: 40, max: 69, color: "#8b5cf6" },
        { label: "70-89", min: 70, max: 89, color: "#f97316" },
        { label: "90-100", min: 90, max: 100, color: "#ef4444" },
    ];

    const counts = bins.map((b) =>
        (nodes || []).filter((n) => {
            const s = Number(n.suspicion_score || 0);
            return s >= b.min && s <= b.max;
        }).length,
    );

    const max = Math.max(1, ...counts);

    bins.forEach((bin, i) => {
        const row = document.createElement("div");
        row.className = "score-row";
        const width = (counts[i] / max) * 100;
        row.innerHTML = `
            <span>${bin.label}</span>
            <div class="score-track"><div class="score-fill" style="width:${width}%; background:${bin.color};"></div></div>
            <span>${counts[i]}</span>
        `;
        host.appendChild(row);
    });
}

function buildRecentRows(edges) {
    return (edges || [])
        .slice()
        .sort((a, b) => String(b.last_timestamp || "").localeCompare(String(a.last_timestamp || "")))
        .map((e) => ({
            tx: (e.sample_transaction_ids && e.sample_transaction_ids.length) ? e.sample_transaction_ids.join(", ") : `${e.source}->${e.target}`,
            sender: e.source,
            receiver: e.target,
            count: Number(e.transaction_count || 0),
            amount: Number(e.total_amount || 0),
            last: e.last_timestamp || "-",
        }));
}

function renderRecentTransactions(rows) {
    const tbody = document.getElementById("recent-transactions-body");
    const countEl = document.getElementById("tx-count");
    if (!tbody) return;

    tbody.innerHTML = "";
    if (!rows || rows.length === 0) {
        tbody.innerHTML = "<tr class='placeholder-row'><td colspan='6'>No transactions to display.</td></tr>";
        if (countEl) countEl.textContent = "Showing 0 transactions";
        return;
    }

    const pageRows = rows.slice(0, TX_PAGE_SIZE);
    pageRows.forEach((r) => {
        const tr = document.createElement("tr");
        tr.innerHTML = `
            <td class="mono">${r.tx}</td>
            <td>${r.sender}</td>
            <td>${r.receiver}</td>
            <td>${numberFmt(r.count)}</td>
            <td>$${numberFmt(r.amount, 2)}</td>
            <td>${r.last}</td>
        `;
        tbody.appendChild(tr);
    });

    if (countEl) countEl.textContent = `Showing ${rows.length} transactions`;
}

function patternTagLabel(pattern) {
    if (pattern && pattern.startsWith("cycle_length_")) {
        return `Cycle ${pattern.replace("cycle_length_", "")}`;
    }
    const labels = {
        smurfing_fan_in: "Fan-in",
        smurfing_fan_out: "Fan-out",
        layered_shell: "Shell",
    };
    return labels[pattern] || pattern;
}

function renderSuspiciousAccounts(accounts) {
    const list = document.getElementById("suspicious-accounts-list");
    const countEl = document.getElementById("sus-count");
    if (!list) return;
    list.innerHTML = "";

    const rows = accounts || [];
    if (countEl) countEl.textContent = `${rows.length} flagged`;

    if (rows.length === 0) {
        list.innerHTML = "<p class='placeholder'>No suspicious accounts detected in this dataset.</p>";
        return;
    }

    rows.forEach((acc) => {
        const score = Number(acc.suspicion_score || 0);
        const card = document.createElement("div");
        card.className = "account-card";
        card.dataset.accountId = acc.account_id;
        card.dataset.patterns = (acc.patterns || []).join(",");

        const tags = (acc.patterns || []).length
            ? (acc.patterns || []).map((p) => `<span class=\"tag\">${patternTagLabel(p)}</span>`).join(" ")
            : "<span class='tag tag-muted'>No pattern flags</span>";

        card.innerHTML = `
            <div class="account-card-header">
                <span>${acc.account_id}</span>
                <span class="account-score">${score.toFixed(1)}</span>
            </div>
            <div class="progress-bar small">
                <div class="progress-fill" style="width:${Math.max(0, Math.min(100, score))}%; background:${scoreColor(score)};"></div>
            </div>
            <div>${tags}</div>
        `;

        card.addEventListener("click", () => {
            if (cyInstance) {
                const node = cyInstance.getElementById(acc.account_id);
                if (node && node.nonempty()) {
                    cyInstance.animate({ fit: { eles: node, padding: 70 }, duration: 260 });
                }
            }
            updateAccountDetails(acc);
        });

        list.appendChild(card);
    });
}

function updateAccountDetails(account) {
    const panel = document.getElementById("account-details");
    if (!panel) return;

    const placeholder = panel.querySelector(".placeholder");
    const details = panel.querySelector(".details-list");
    if (placeholder) placeholder.hidden = true;
    if (details) details.hidden = false;

    setText(document.getElementById("detail-account-id"), account.account_id || "");

    const score = Number(account.suspicion_score || 0);
    const fill = document.getElementById("detail-score-fill");
    if (fill) {
        fill.style.width = `${Math.max(0, Math.min(100, score))}%`;
        fill.style.background = scoreColor(score);
    }

    setText(document.getElementById("detail-score-text"), `${score.toFixed(1)} / 100`);

    const patternsEl = document.getElementById("detail-patterns");
    if (patternsEl) {
        const patterns = account.patterns || [];
        patternsEl.innerHTML = patterns.length
            ? patterns.map((p) => `<span class='tag'>${patternTagLabel(p)}</span>`).join(" ")
            : "<span class='tag tag-muted'>No pattern flags</span>";
    }

    setText(document.getElementById("detail-ring"), account.ring_id || "Not part of any detected ring");
    setText(document.getElementById("detail-degree"), `${account.in_degree || 0} / ${account.out_degree || 0}`);
    setText(document.getElementById("detail-flow"), `${numberFmt(account.total_in_amount || 0, 2)} / ${numberFmt(account.total_out_amount || 0, 2)}`);
}

function buildDownloadPayload() {
    if (!latestAnalysis) {
        return {
            suspicious_accounts: [],
            fraud_rings: [],
            summary: {
                total_accounts_analyzed: 0,
                suspicious_accounts_flagged: 0,
                fraud_rings_detected: 0,
                processing_time_seconds: 0,
            },
        };
    }

    return {
        suspicious_accounts: (latestAnalysis.suspicious_accounts || []).map((a) => ({
            account_id: a.account_id,
            suspicion_score: a.suspicion_score,
            detected_patterns: a.patterns || [],
            ring_id: a.ring_id || "",
        })),
        fraud_rings: (latestAnalysis.fraud_rings || []).map((r) => ({
            ring_id: r.ring_id,
            member_accounts: r.member_accounts || [],
            pattern_type: r.pattern_type || "cycle",
            risk_score: r.risk_score,
        })),
        summary: {
            total_accounts_analyzed: latestAnalysis.summary_stats?.total_accounts || 0,
            suspicious_accounts_flagged: latestAnalysis.summary_stats?.suspicious_accounts || 0,
            fraud_rings_detected: latestAnalysis.summary_stats?.fraud_rings || 0,
            processing_time_seconds: latestAnalysis.summary_stats?.processing_time_seconds || 0,
        },
    };
}

function attachDownloadHandler() {
    const btn = document.getElementById("download-json-btn");
    if (!btn) return;

    btn.addEventListener("click", () => {
        if (!latestAnalysis) return;
        const payload = buildDownloadPayload();
        const blob = new Blob([JSON.stringify(payload, null, 2)], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "rift_investigation_report.json";
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    });
}

function attachSearchHandlers() {
    const accountInput = document.getElementById("account-search");
    if (accountInput) {
        accountInput.addEventListener("input", () => {
            const term = accountInput.value.trim().toLowerCase();
            const cards = document.querySelectorAll("#suspicious-accounts-list .account-card");
            cards.forEach((card) => {
                const id = (card.dataset.accountId || "").toLowerCase();
                const patterns = (card.dataset.patterns || "").toLowerCase();
                card.style.display = (!term || id.includes(term) || patterns.includes(term)) ? "" : "none";
            });
        });
    }

    const txInput = document.getElementById("tx-search");
    if (txInput) {
        txInput.addEventListener("input", () => {
            const term = txInput.value.trim().toLowerCase();
            const filtered = !term
                ? recentRows
                : recentRows.filter((r) =>
                    String(r.tx).toLowerCase().includes(term)
                    || String(r.sender).toLowerCase().includes(term)
                    || String(r.receiver).toLowerCase().includes(term),
                );
            renderRecentTransactions(filtered);
        });
    }
}

function resetView() {
    updateSummaryCards({
        total_accounts: 0,
        suspicious_accounts: 0,
        fraud_rings: 0,
        processing_time_seconds: 0,
    });
    setText(document.getElementById("stat-nodes"), "0");
    setText(document.getElementById("stat-edges"), "0");
    setText(document.getElementById("stat-degree"), "0");
    setText(document.getElementById("stat-component"), "0");

    renderFraudRingsTable([]);
    renderScoreDistribution([]);
    renderRecentTransactions([]);
    renderSuspiciousAccounts([]);

    const placeholder = document.querySelector("#account-details .placeholder");
    const details = document.querySelector("#account-details .details-list");
    if (placeholder) placeholder.hidden = false;
    if (details) details.hidden = true;

    const dlBtn = document.getElementById("download-json-btn");
    if (dlBtn) dlBtn.disabled = true;

    if (cyInstance) {
        cyInstance.destroy();
        cyInstance = null;
    }
}

function attachUploadHandler() {
    const form = document.getElementById("upload-form");
    const fileInput = document.getElementById("file-input");
    const errorBox = document.getElementById("upload-error");
    const analyzeBtn = document.getElementById("analyze-btn");

    if (!form || !fileInput || !analyzeBtn) return;

    function setError(message) {
        if (!errorBox) return;
        if (!message) {
            errorBox.hidden = true;
            errorBox.textContent = "";
            return;
        }
        errorBox.hidden = false;
        errorBox.textContent = message;
    }

    form.addEventListener("submit", async (event) => {
        event.preventDefault();
        setError("");

        const file = fileInput.files[0];
        if (!file) {
            setError("Please choose a CSV file first.");
            return;
        }
        if (!file.name.toLowerCase().endsWith(".csv")) {
            setError("Invalid file type. Please upload a CSV file.");
            return;
        }
        if (file.size > MAX_UPLOAD_BYTES) {
            setError("CSV exceeds 10 MB upload limit.");
            return;
        }

        const formData = new FormData();
        formData.append("file", file);

        let timeoutId = null;
        try {
            analyzeBtn.disabled = true;
            analyzeBtn.textContent = "Analyzing...";
            resetView();

            const controller = new AbortController();
            timeoutId = setTimeout(() => controller.abort(), CLIENT_TIMEOUT_MS);

            const response = await fetch("/analyze", {
                method: "POST",
                body: formData,
                signal: controller.signal,
            });

            clearTimeout(timeoutId);
            timeoutId = null;

            let payload;
            try {
                payload = await response.json();
            } catch (_) {
                setError("Server returned an invalid response.");
                return;
            }

            if (!response.ok || payload.status !== "ok") {
                setError(payload.message || "Analysis failed. Please verify your CSV.");
                return;
            }

            if (!payload.data || !Array.isArray(payload.data.nodes) || !Array.isArray(payload.data.edges)) {
                setError("Server response is missing required graph data.");
                return;
            }

            latestAnalysis = payload.data;
            recentRows = buildRecentRows(latestAnalysis.edges || []);

            updateSummaryCards(latestAnalysis.summary_stats || {});
            buildGraph(latestAnalysis);
            renderScoreDistribution(latestAnalysis.nodes || []);
            renderFraudRingsTable(latestAnalysis.fraud_rings || []);
            renderRecentTransactions(recentRows);
            renderSuspiciousAccounts(latestAnalysis.suspicious_accounts || []);

            const dlBtn = document.getElementById("download-json-btn");
            if (dlBtn) dlBtn.disabled = false;
        } catch (err) {
            if (err && err.name === "AbortError") {
                setError("Analysis exceeded 30 seconds. Try a smaller CSV.");
            } else {
                setError("Server connection failed. Please try again.");
            }
        } finally {
            if (timeoutId) clearTimeout(timeoutId);
            analyzeBtn.disabled = false;
            analyzeBtn.textContent = "Run Analysis";
        }
    });
}

document.addEventListener("DOMContentLoaded", () => {
    attachUploadHandler();
    attachSearchHandlers();
    attachDownloadHandler();
    resetView();
});
