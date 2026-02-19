let cyInstance = null;
let latestAnalysis = null;

function setText(el, value) {
    if (!el) return;
    el.textContent = value;
}

function animateMetric(el, target) {
    if (!el) return;
    const duration = 600;
    const start = performance.now();
    const from = 0;
    const to = Number(target) || 0;

    function frame(now) {
        const t = Math.min(1, (now - start) / duration);
        const eased = t * (2 - t); // easeOutQuad
        const value = Math.round(from + (to - from) * eased);
        el.textContent = value.toLocaleString();
        if (t < 1) {
            requestAnimationFrame(frame);
        }
    }

    requestAnimationFrame(frame);
}

function updateSummaryCards(summary) {
    if (!summary) return;
    animateMetric(
        document.querySelector("#metric-total-transactions .metric-value"),
        summary.total_transactions,
    );
    animateMetric(
        document.querySelector("#metric-total-accounts .metric-value"),
        summary.total_accounts,
    );
    animateMetric(
        document.querySelector("#metric-suspicious-accounts .metric-value"),
        summary.suspicious_accounts,
    );
    animateMetric(
        document.querySelector("#metric-fraud-rings .metric-value"),
        summary.fraud_rings,
    );
    animateMetric(
        document.querySelector("#metric-highest-risk .metric-value"),
        summary.highest_risk_score.toFixed
            ? summary.highest_risk_score.toFixed(0)
            : summary.highest_risk_score,
    );
}

function buildGraph(data) {
    const container = document.getElementById("graph-container");
    if (!container) return;

    const elements = [];

    (data.nodes || []).forEach((node) => {
        elements.push({
            data: {
                id: node.account_id,
                label: node.account_id,
                suspicion_score: node.suspicion_score,
                is_suspicious: !!node.is_suspicious,
                ring_id: node.ring_id || "",
                patterns: (node.patterns || []).join(","),
            },
        });
    });

    (data.edges || []).forEach((edge) => {
        elements.push({
            data: {
                source: edge.source,
                target: edge.target,
                transaction_count: edge.transaction_count,
                total_amount: edge.total_amount,
            },
        });
    });

    if (cyInstance) {
        cyInstance.destroy();
    }

    cyInstance = cytoscape({
        container,
        elements,
        style: [
            {
                selector: "node",
                style: {
                    "label": "data(label)",
                    "color": "#e5f2ff",
                    "font-size": "10px",
                    "text-valign": "center",
                    "text-outline-width": 1,
                    "text-outline-color": "rgba(15,23,42,0.9)",
                    "background-color": "#38bdf8",
                    "width": "24px",
                    "height": "24px",
                    "border-width": 1,
                    "border-color": "rgba(148, 163, 184, 0.6)",
                    "transition-property": "background-color, width, height, box-shadow",
                    "transition-duration": "200ms",
                },
            },
            {
                selector: "node[is_suspicious = 1], node[is_suspicious = true]",
                style: {
                    "background-color": "#f97373",
                    "width": "34px",
                    "height": "34px",
                    "box-shadow": "0 0 18px rgba(248, 113, 113, 0.9)",
                },
            },
            {
                selector: "node[ring_id != '']",
                style: {
                    "border-width": 2,
                    "border-color": "#facc15",
                },
            },
            {
                selector: "edge",
                style: {
                    "width": 1.5,
                    "line-color": "rgba(148,163,184,0.6)",
                    "curve-style": "bezier",
                    "target-arrow-shape": "triangle",
                    "target-arrow-color": "rgba(148,163,184,0.9)",
                    "arrow-scale": 0.8,
                },
            },
            {
                selector: "edge[transaction_count > 3]",
                style: {
                    "width": 2.2,
                    "line-color": "rgba(59,130,246,0.9)",
                    "target-arrow-color": "rgba(59,130,246,0.9)",
                },
            },
        ],
        layout: {
            name: "cose",
            idealEdgeLength: 120,
            nodeRepulsion: 8000,
            gravity: 0.8,
            numIter: 1000,
            animate: true,
        },
        wheelSensitivity: 0.2,
    });

    const tooltip = document.getElementById("graph-tooltip");
    function showTooltip(node, event) {
        if (!tooltip) return;
        const data = node.data();
        const patterns = (data.patterns || "")
            .split(",")
            .filter(Boolean)
            .map((p) => `<span class="tag tag-compact">${p}</span>`)
            .join(" ");

        tooltip.innerHTML = `
            <div class="tooltip-title">${data.id}</div>
            <div class="tooltip-row">
                <span>Suspicion score</span>
                <span>${Number(data.suspicion_score || 0).toFixed(0)}</span>
            </div>
            <div class="tooltip-row">
                <span>Ring</span>
                <span>${data.ring_id || "—"}</span>
            </div>
            <div class="tooltip-row tooltip-row-tags">
                <span>Patterns</span>
                <span>${patterns || "—"}</span>
            </div>
        `;

        const pos = event.renderedPosition || event.position || { x: 0, y: 0 };
        tooltip.style.left = `${pos.x + 16}px`;
        tooltip.style.top = `${pos.y + 16}px`;
        tooltip.hidden = false;
    }

    function hideTooltip() {
        if (tooltip) {
            tooltip.hidden = true;
        }
    }

    cyInstance.on("mouseover", "node", (evt) => showTooltip(evt.target, evt));
    cyInstance.on("mouseout", "node", hideTooltip);
    cyInstance.on("drag", "node", hideTooltip);
    cyInstance.on("zoom pan", hideTooltip);

    cyInstance.on("tap", "node", (evt) => {
        const node = evt.target;
        const id = node.id();
        if (!latestAnalysis) return;
        const account = (latestAnalysis.nodes || []).find(
            (n) => n.account_id === id,
        );
        if (account) {
            updateAccountDetails(account);
        }
    });
}

function renderFraudRingsTable(rings) {
    const tbody = document.querySelector("#fraud-rings-table tbody");
    if (!tbody) return;
    tbody.innerHTML = "";

    if (!rings || rings.length === 0) {
        const row = document.createElement("tr");
        row.className = "placeholder-row";
        const cell = document.createElement("td");
        cell.colSpan = 5;
        cell.textContent = "No fraud rings detected.";
        row.appendChild(cell);
        tbody.appendChild(row);
        return;
    }

    rings
        .slice()
        .sort((a, b) => (b.risk_score || 0) - (a.risk_score || 0))
        .forEach((ring) => {
            const tr = document.createElement("tr");

            const members = (ring.member_accounts || []).join(", ");

            tr.innerHTML = `
                <td>${ring.ring_id}</td>
                <td>${ring.member_count}</td>
                <td>
                    <div class="progress-bar">
                        <div class="progress-fill" style="width:${ring.risk_score}%;"></div>
                    </div>
                    <span class="progress-label">${ring.risk_score.toFixed
                        ? ring.risk_score.toFixed(0)
                        : ring.risk_score}</span>
                </td>
                <td>${ring.pattern_type || "fraud_ring"}</td>
                <td class="mono">${members}</td>
            `;

            tbody.appendChild(tr);
        });
}

function patternTagLabel(pattern) {
    if (pattern && pattern.startsWith("cycle_length_")) {
        return "Cycle " + pattern.replace("cycle_length_", "");
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
    if (!list) return;
    list.innerHTML = "";

    if (!accounts || accounts.length === 0) {
        const p = document.createElement("p");
        p.className = "placeholder";
        p.textContent = "No suspicious accounts detected in this dataset.";
        list.appendChild(p);
        return;
    }

    accounts.forEach((acc) => {
        const card = document.createElement("div");
        card.className = "account-card";
        card.dataset.accountId = acc.account_id;
        card.dataset.patterns = (acc.patterns || []).join(",");

        const tags =
            (acc.patterns || [])
                .map(
                    (p) =>
                        `<span class="tag">${patternTagLabel(
                            p,
                        )}</span>`,
                )
                .join(" ") || "<span class=\"tag tag-muted\">No pattern flags</span>";

        const score = Number(acc.suspicion_score || 0);

        card.innerHTML = `
            <div class="account-card-header">
                <div class="account-id">${acc.account_id}</div>
                <div class="account-score">${score.toFixed(0)}</div>
            </div>
            <div class="progress-bar small">
                <div class="progress-fill" style="width:${score}%"></div>
            </div>
            <div class="account-tags">${tags}</div>
        `;

        card.addEventListener("click", () => {
            if (cyInstance) {
                const node = cyInstance.getElementById(acc.account_id);
                if (node && node.nonempty()) {
                    cyInstance.fit(node, 80);
                    node.flashAnimation = true;
                    node.addClass("selected");
                    setTimeout(() => node.removeClass("selected"), 600);
                }
            }
            updateAccountDetails(acc);
        });

        list.appendChild(card);
    });
}

function updateAccountDetails(account) {
    const container = document.getElementById("account-details");
    if (!container) return;
    const placeholder = container.querySelector(".placeholder");
    const details = container.querySelector(".details-list");
    if (placeholder) placeholder.hidden = true;
    if (details) details.hidden = false;

    setText(document.getElementById("detail-account-id"), account.account_id);

    const score = Number(account.suspicion_score || 0);
    const scoreFill = document.getElementById("detail-score-fill");
    if (scoreFill) {
        scoreFill.style.width = `${score}%`;
    }
    setText(
        document.getElementById("detail-score-text"),
        `${score.toFixed(0)} / 100`,
    );

    const patternsContainer = document.getElementById("detail-patterns");
    if (patternsContainer) {
        const patterns = account.patterns || [];
        if (!patterns.length) {
            patternsContainer.innerHTML =
                '<span class="tag tag-muted">No pattern flags</span>';
        } else {
            patternsContainer.innerHTML = patterns
                .map(
                    (p) =>
                        `<span class="tag">${patternTagLabel(
                            p,
                        )}</span>`,
                )
                .join(" ");
        }
    }

    setText(
        document.getElementById("detail-ring"),
        account.ring_id || "Not part of any detected ring",
    );
    setText(
        document.getElementById("detail-degree"),
        `${account.in_degree || 0} / ${account.out_degree || 0}`,
    );
    setText(
        document.getElementById("detail-flow"),
        `${(account.total_in_amount || 0).toLocaleString()} / ${(account.total_out_amount || 0).toLocaleString()}`,
    );
}

function attachSearchHandler() {
    const input = document.getElementById("account-search");
    if (!input) return;
    input.addEventListener("input", () => {
        const term = input.value.toLowerCase();
        const cards = document.querySelectorAll(
            "#suspicious-accounts-list .account-card",
        );
        cards.forEach((card) => {
            const id = (card.dataset.accountId || "").toLowerCase();
            const patterns = (card.dataset.patterns || "").toLowerCase();
            const match =
                !term || id.includes(term) || patterns.includes(term);
            card.style.display = match ? "" : "none";
        });
    });
}

function attachUploadHandler() {
    const form = document.getElementById("upload-form");
    const fileInput = document.getElementById("file-input");
    const errorBox = document.getElementById("upload-error");
    const analyzeBtn = document.getElementById("analyze-btn");

    if (!form || !fileInput) return;

    function setError(message) {
        if (!errorBox) return;
        if (!message) {
            errorBox.hidden = true;
            errorBox.textContent = "";
        } else {
            errorBox.hidden = false;
            errorBox.textContent = message;
        }
    }

    form.addEventListener("submit", async (e) => {
        e.preventDefault();
        setError("");

        const file = fileInput.files[0];
        if (!file) {
            setError("Please select a CSV file to analyze.");
            return;
        }
        if (!file.name.toLowerCase().endsWith(".csv")) {
            setError("Invalid file type. Please upload a .csv file.");
            return;
        }

        const formData = new FormData();
        formData.append("file", file);

        try {
            if (analyzeBtn) {
                analyzeBtn.disabled = true;
                analyzeBtn.textContent = "Analyzing…";
            }

            const response = await fetch("/analyze", {
                method: "POST",
                body: formData,
            });
            const payload = await response.json();

            if (!response.ok || payload.status !== "ok") {
                const msg =
                    (payload && payload.message) ||
                    "Analysis failed. Please verify your CSV file.";
                setError(msg);
                return;
            }

            latestAnalysis = payload.data;
            updateSummaryCards(latestAnalysis.summary_stats);
            buildGraph(latestAnalysis);
            renderFraudRingsTable(latestAnalysis.fraud_rings);
            renderSuspiciousAccounts(latestAnalysis.suspicious_accounts);
            const dlBtn = document.getElementById("download-json-btn");
            if (dlBtn) {
                dlBtn.disabled = false;
            }
        } catch (err) {
            setError(
                "Unexpected error while contacting the server. Please try again.",
            );
        } finally {
            if (analyzeBtn) {
                analyzeBtn.disabled = false;
                analyzeBtn.textContent = "Run Detection";
            }
        }
    });
}

function buildHackathonJsonPayload() {
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

    const suspicious = (latestAnalysis.suspicious_accounts || []).map((acc) => {
        return {
            account_id: acc.account_id,
            suspicion_score: acc.suspicion_score,
            detected_patterns: acc.patterns || [],
            ring_id: acc.ring_id || "",
        };
    });

    const rings = (latestAnalysis.fraud_rings || []).map((ring) => {
        return {
            ring_id: ring.ring_id,
            member_accounts: ring.member_accounts || [],
            pattern_type: "cycle",
            risk_score: ring.risk_score,
        };
    });

    const summaryStats = latestAnalysis.summary_stats || {};
    const summary = {
        total_accounts_analyzed: summaryStats.total_accounts || 0,
        suspicious_accounts_flagged: summaryStats.suspicious_accounts || 0,
        fraud_rings_detected: summaryStats.fraud_rings || 0,
        processing_time_seconds: summaryStats.processing_time_seconds || 0,
    };

    return {
        suspicious_accounts: suspicious,
        fraud_rings: rings,
        summary,
    };
}

function attachDownloadHandler() {
    const btn = document.getElementById("download-json-btn");
    if (!btn) return;

    btn.addEventListener("click", () => {
        if (!latestAnalysis) {
            return;
        }
        const payload = buildHackathonJsonPayload();
        const blob = new Blob([JSON.stringify(payload, null, 2)], {
            type: "application/json",
        });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = "rift_money_muling_result.json";
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    });
}

document.addEventListener("DOMContentLoaded", () => {
    attachUploadHandler();
    attachSearchHandler();
    attachDownloadHandler();
});

