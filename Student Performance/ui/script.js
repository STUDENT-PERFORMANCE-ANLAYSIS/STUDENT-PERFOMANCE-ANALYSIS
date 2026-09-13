// ui/script.js
/**
 * Frontend script handling user interaction and AJAX communications with the Flask backend.
 * 
 * Why this file exists:
 * Modern web applications rely on asynchronous communication to deliver a fluid 
 * user experience. Instead of reloading the entire web page every time a user requests 
 * an analysis (which would destroy Spark context variables and restart the session), 
 * this script uses the Fetch API (Asynchronous JavaScript and XML - AJAX concept) to send 
 * background requests to Flask, receive JSON payloads, and dynamically update the DOM.
 * 
 * Viva Tips:
 * - What is asynchronous execution (AJAX)? The process of fetching data from a server 
 *   asynchronously without interrupting the active display or triggering a page refresh.
 * - What is DOM Manipulation? Document Object Model manipulation. It is JavaScript's 
 *   ability to add, remove, or modify HTML elements dynamically on the page (e.g., building table rows).
 * - What is the Fetch API? A modern promise-based browser interface for making HTTP requests.
 */

// Global tracking of the active query type for CSV export
let currentActiveQuery = null;

// Global tracking for Chart.js chart instance
let chartInstance = null;

// Global timer for backend auto-reconnect polling
let autoRetryTimer = null;

// Backend URL pointing directly to the Flask server (bypasses Live Server port mismatch)
const BACKEND_URL = 'http://127.0.0.1:5000';

// Automatically check server dataset status on load
checkStatus();

/**
 * Checks connectivity to Flask backend and retrieves active dataset status.
 * Automatically handles offline status and sets auto-reconnect timer.
 */
function checkStatus() {
    updateStatus("loading", "Connecting to Spark Backend...");

    fetch(`${BACKEND_URL}/status`)
    .then(response => {
        if (!response.ok) {
            throw new Error(`Server returned HTTP status ${response.status}`);
        }
        return response.json();
    })
    .then(data => {
        // Clear auto-reconnect polling if connection established
        if (autoRetryTimer) {
            clearInterval(autoRetryTimer);
            autoRetryTimer = null;
        }
        removeOfflineAlert();

        if (data.status === "ready") {
            updateStatus("ready", `Dataset Loaded: ${data.filename} (${data.count} records)`);
            enableControls(true);
            fetchPreview();
        } else {
            updateStatus("no_data", data.message || "No dataset loaded.");
            enableControls(false);
        }
    })
    .catch(error => {
        updateStatus("error", "Backend Server Offline (Port 5000) - Auto-retrying...");
        enableControls(false);
        showOfflineAlert();

        // Poll connection every 3 seconds until server comes up
        if (!autoRetryTimer) {
            autoRetryTimer = setInterval(() => {
                checkStatus();
            }, 3000);
        }
    });
}

/**
 * Shows server offline alert panel with helpful debugging instructions.
 */
function showOfflineAlert() {
    let alertBox = document.getElementById('offline-alert-box');
    if (!alertBox) {
        const controlPanel = document.getElementById('control-panel');
        alertBox = document.createElement('div');
        alertBox.id = 'offline-alert-box';
        alertBox.className = 'offline-card';
        alertBox.innerHTML = `
            <div class="offline-title">
                <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                    <circle cx="12" cy="12" r="10"></circle>
                    <line x1="12" y1="8" x2="12" y2="12"></line>
                    <line x1="12" y1="16" x2="12.01" y2="16"></line>
                </svg>
                Flask Server Disconnected
            </div>
            <div class="offline-text">
                The Python Spark backend server is not running on <strong>http://127.0.0.1:5000</strong>.
                <br>• Auto-retrying connection every 3 seconds...
                <br>• Double click <strong>manage_server.bat</strong> in project folder to start backend manually.
            </div>
            <button class="btn btn-secondary retry-btn" onclick="checkStatus()">Retry Now</button>
        `;
        controlPanel.appendChild(alertBox);
    }
}

/**
 * Removes server offline alert once backend reconnects.
 */
function removeOfflineAlert() {
    const alertBox = document.getElementById('offline-alert-box');
    if (alertBox) {
        alertBox.remove();
    }
}

/**
 * Fetches a raw tabular preview of the first few rows of the dataset.
 */
function fetchPreview() {
    showLoader("Loading raw Spark DataFrame preview...");
    currentActiveQuery = null; // Can't export raw preview directly
    document.getElementById('export-controls').style.display = 'none';
    if (document.getElementById('chart-panel')) {
        document.getElementById('chart-panel').style.display = 'none';
    }

    fetch(`${BACKEND_URL}/preview`)
    .then(response => response.json())
    .then(data => {
        hideLoader();
        renderTable(data.columns, data.data);
    })
    .catch(error => {
        hideLoader();
        showErrorInTable("Failed to load preview: " + error.message);
    });
}

/**
 * Sends a query request to Flask backend to trigger PySpark DataFrame analytics.
 * @param {string} queryType - Key representing the analysis to run (e.g. 'top10', 'dept_avg', 'pass_fail')
 */
function runAnalysis(queryType) {
    showLoader(`Spark executing transformation plan for: ${queryType}...`);
    currentActiveQuery = queryType;
    
    // Hide panels initially
    document.getElementById('stats-display').style.display = 'none';
    document.getElementById('chart-panel').style.display = 'none';
    document.getElementById('table-container').style.display = 'block';
    document.getElementById('export-controls').style.display = 'none';

    fetch(`${BACKEND_URL}/analyze/${queryType}`)
    .then(response => {
        if (!response.ok) {
            return response.json().then(err => { throw new Error(err.error || "Analytics failed"); });
        }
        return response.json();
    })
    .then(data => {
        hideLoader();
        
        // Show export controls once analysis completes successfully
        document.getElementById('export-controls').style.display = 'flex';

        // Render appropriate visualization (metrics cards, charts, or table)
        if (queryType === 'class_stats') {
            renderMetrics(data.data);
            renderTable(data.columns, data.data);
        } else {
            renderTable(data.columns, data.data);
            renderChart(queryType, data.columns, data.data);
        }
    })
    .catch(error => {
        hideLoader();
        showErrorInTable(`Analytical execution failed: ${error.message}`);
    });
}

/**
 * Renders visual graphs and charts using Chart.js for Pass/Fail and analytics reports.
 */
function renderChart(queryType, headers, rows) {
    const chartPanel = document.getElementById('chart-panel');
    const chartTitle = document.getElementById('chart-title');
    const canvas = document.getElementById('analytics-chart');
    
    if (!canvas || !rows || rows.length === 0) {
        if (chartPanel) chartPanel.style.display = 'none';
        return;
    }

    // Safely destroy previous Chart instance to prevent canvas overlap/glitches
    if (chartInstance) {
        chartInstance.destroy();
        chartInstance = null;
    }

    // Check if Chart.js library is loaded
    if (typeof Chart === 'undefined') {
        console.warn('Chart.js library is unavailable. Displaying tabular output only.');
        if (chartPanel) chartPanel.style.display = 'none';
        return;
    }

    let labels = [];
    let datasets = [];
    let chartType = 'bar';
    let titleText = 'Visual Analytics Graph';

    if (queryType === 'pass_fail') {
        titleText = 'Pass vs Fail Statistics Visual Graph';
        chartType = 'doughnut';
        
        labels = rows.map(r => `${r.Status} (${r.Percentage}%)`);
        const counts = rows.map(r => r.Count);
        
        const backgroundColors = rows.map(r => String(r.Status).toLowerCase().includes('pass') ? 'rgba(16, 185, 129, 0.85)' : 'rgba(244, 63, 94, 0.85)');
        const borderColors = rows.map(r => String(r.Status).toLowerCase().includes('pass') ? '#10b981' : '#f43f5e');

        datasets = [{
            label: 'Student Count',
            data: counts,
            backgroundColor: backgroundColors,
            borderColor: borderColors,
            borderWidth: 2,
            hoverOffset: 12
        }];
    } else if (queryType === 'dept_avg') {
        titleText = 'Department-wise Average Marks';
        chartType = 'bar';
        labels = rows.map(r => r.Department);
        datasets = [{
            label: 'Average Marks',
            data: rows.map(r => r.Average_Marks),
            backgroundColor: 'rgba(99, 102, 241, 0.75)',
            borderColor: '#6366f1',
            borderWidth: 2,
            borderRadius: 6
        }];
    } else if (queryType === 'subject_avg') {
        titleText = 'Subject-wise Average Marks';
        chartType = 'bar';
        labels = rows.map(r => r.Subject);
        datasets = [{
            label: 'Average Marks',
            data: rows.map(r => r.Average_Marks),
            backgroundColor: 'rgba(245, 158, 11, 0.75)',
            borderColor: '#f59e0b',
            borderWidth: 2,
            borderRadius: 6
        }];
    } else if (queryType === 'grade_dist') {
        titleText = 'Grade Distribution';
        chartType = 'bar';
        labels = rows.map(r => `Grade ${r.Grade}`);
        datasets = [{
            label: 'Student Count',
            data: rows.map(r => r.Student_Count),
            backgroundColor: 'rgba(167, 139, 250, 0.75)',
            borderColor: '#a78bfa',
            borderWidth: 2,
            borderRadius: 6
        }];
    } else if (queryType === 'sem_perf') {
        titleText = 'Semester-wise Performance & Attendance';
        chartType = 'bar';
        labels = rows.map(r => `Semester ${r.Semester}`);
        datasets = [
            {
                label: 'Avg Marks',
                data: rows.map(r => r.Average_Marks),
                backgroundColor: 'rgba(99, 102, 241, 0.75)',
                borderColor: '#6366f1',
                borderWidth: 1.5,
                borderRadius: 4
            },
            {
                label: 'Avg Attendance (%)',
                data: rows.map(r => r.Average_Attendance),
                backgroundColor: 'rgba(16, 185, 129, 0.75)',
                borderColor: '#10b981',
                borderWidth: 1.5,
                borderRadius: 4
            }
        ];
    } else if (queryType === 'attendance') {
        titleText = 'Attendance Shortage (< 75%) Students';
        chartType = 'bar';
        labels = rows.slice(0, 15).map(r => r.Student_Name || r.Student_ID);
        datasets = [{
            label: 'Attendance (%)',
            data: rows.slice(0, 15).map(r => r.Attendance),
            backgroundColor: 'rgba(244, 63, 94, 0.75)',
            borderColor: '#f43f5e',
            borderWidth: 2,
            borderRadius: 6
        }];
    } else if (queryType === 'top10') {
        titleText = 'Top 10 Scoring Students';
        chartType = 'bar';
        labels = rows.map(r => r.Student_Name);
        datasets = [{
            label: 'Marks',
            data: rows.map(r => r.Marks),
            backgroundColor: 'rgba(16, 185, 129, 0.75)',
            borderColor: '#10b981',
            borderWidth: 2,
            borderRadius: 6
        }];
    } else {
        // Hide panel if no specific graph defined for query type
        chartPanel.style.display = 'none';
        return;
    }

    chartPanel.style.display = 'block';
    chartTitle.textContent = titleText;

    const ctx = canvas.getContext('2d');
    chartInstance = new Chart(ctx, {
        type: chartType,
        data: {
            labels: labels,
            datasets: datasets
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: true,
                    labels: {
                        color: '#f3f4f6',
                        font: { family: 'Inter', size: 12 }
                    }
                },
                tooltip: {
                    backgroundColor: 'rgba(26, 29, 46, 0.95)',
                    titleColor: '#fff',
                    bodyColor: '#f3f4f6',
                    borderColor: 'rgba(255, 255, 255, 0.1)',
                    borderWidth: 1
                }
            },
            scales: (chartType === 'doughnut' || chartType === 'pie') ? {} : {
                x: {
                    ticks: { color: '#9ca3af', font: { family: 'Inter' } },
                    grid: { color: 'rgba(255, 255, 255, 0.05)' }
                },
                y: {
                    ticks: { color: '#9ca3af', font: { family: 'Inter' } },
                    grid: { color: 'rgba(255, 255, 255, 0.05)' },
                    beginAtZero: true
                }
            }
        }
    });
}

/**
 * Triggers backend export logic, saving the current active query to output/ folder as CSV.
 */
function exportReport() {
    if (!currentActiveQuery) {
        alert("Please run an analytical report first before exporting.");
        return;
    }

    showLoader(`Writing partitions to CSV format for: ${currentActiveQuery}...`);

    fetch(`${BACKEND_URL}/export/${currentActiveQuery}`)
    .then(response => {
        if (!response.ok) {
            return response.json().then(err => { throw new Error(err.error || "Export failed"); });
        }
        return response.json();
    })
    .then(data => {
        hideLoader();
        alert(`Report successfully exported to: ${data.path}`);
    })
    .catch(error => {
        hideLoader();
        alert(`Failed to export report: ${error.message}`);
    });
}

/**
 * Helper: Renders rows and headers dynamically inside the HTML table.
 */
function renderTable(headers, rows) {
    const tableHeader = document.getElementById('table-header');
    const tableBody = document.getElementById('table-body');
    
    // Clear previous elements
    tableHeader.innerHTML = '';
    tableBody.innerHTML = '';

    if (!headers || headers.length === 0) {
        tableBody.innerHTML = `<tr><td colspan="100%" style="text-align: center;">No fields found.</td></tr>`;
        return;
    }

    // Append table headers
    headers.forEach(header => {
        const th = document.createElement('th');
        th.textContent = header.replace(/_/g, ' '); // Clean underscores for display
        tableHeader.appendChild(th);
    });

    if (!rows || rows.length === 0) {
        tableBody.innerHTML = `<tr><td colspan="${headers.length}" style="text-align: center; color: var(--text-secondary);">Empty dataset returned by Spark.</td></tr>`;
        return;
    }

    // Append table rows
    rows.forEach(row => {
        const tr = document.createElement('tr');
        headers.forEach(header => {
            const td = document.createElement('td');
            // Safely retrieve property or render empty cell
            const val = row[header];
            td.textContent = (val !== null && val !== undefined) ? val : '-';
            tr.appendChild(td);
        });
        tableBody.appendChild(tr);
    });
}

/**
 * Helper: Renders overall statistics summary inside visual cards.
 */
function renderMetrics(stats) {
    const statsDisplay = document.getElementById('stats-display');
    statsDisplay.innerHTML = '';
    statsDisplay.style.display = 'grid';

    stats.forEach(item => {
        const card = document.createElement('div');
        card.className = 'metric-card';
        
        const value = document.createElement('div');
        value.className = 'metric-value';
        // Check if value is percentage or count
        if (item.Metric.includes("Percentage")) {
            value.textContent = `${item.Value}%`;
        } else if (item.Metric.includes("Average") || item.Metric.includes("Obtained")) {
            value.textContent = Number(item.Value).toFixed(2);
        } else {
            value.textContent = Math.round(item.Value);
        }

        const label = document.createElement('div');
        label.className = 'metric-label';
        label.textContent = item.Metric;

        card.appendChild(value);
        card.appendChild(label);
        statsDisplay.appendChild(card);
    });
}

/**
 * UI Utilities: Controls button activation states.
 */
function enableControls(enabled) {
    const buttons = document.querySelectorAll('#analytics-buttons-wrapper button');
    buttons.forEach(button => {
        button.disabled = !enabled;
    });
}

/**
 * UI Utilities: Controls progress spinner.
 */
function showLoader(message) {
    document.getElementById('query-loader').style.display = 'flex';
    document.getElementById('loader-message').textContent = message;
    document.getElementById('table-container').style.display = 'none';
}

function hideLoader() {
    document.getElementById('query-loader').style.display = 'none';
    document.getElementById('table-container').style.display = 'block';
}

/**
 * UI Utilities: Adjusts session status indicator.
 */
function updateStatus(state, message) {
    const dot = document.getElementById('status-dot');
    const text = document.getElementById('status-text');

    dot.className = "status-indicator"; // Reset class
    text.textContent = message;

    if (state === "ready") {
        dot.classList.add("ready");
    } else if (state === "loading") {
        dot.style.background = "var(--accent-amber)";
    } else if (state === "error") {
        dot.style.background = "var(--accent-rose)";
    }
}

/**
 * UI Utilities: Appends error row inside table.
 */
function showErrorInTable(errorMessage) {
    const tableHeader = document.getElementById('table-header');
    const tableBody = document.getElementById('table-body');
    tableHeader.innerHTML = '<th>Error Information</th>';
    tableBody.innerHTML = `<tr><td style="color: var(--accent-rose); font-weight: 500; text-align: center;">${errorMessage}</td></tr>`;
}
