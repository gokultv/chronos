const searchBtn = document.getElementById('searchBtn');
const sourceInput = document.getElementById('sourceInput');
const containsInput = document.getElementById('containsInput');
const resultsTable = document.getElementById('resultsTable').querySelector('tbody');
const statsBar = document.getElementById('statsBar');
const statEvents = document.getElementById('statEvents');
const statDuration = document.getElementById('statDuration');
const statMatches = document.getElementById('statMatches');

searchBtn.addEventListener('click', performSearch);

// Allow Enter key to trigger search
sourceInput.addEventListener('keypress', (e) => { if (e.key === 'Enter') performSearch() });
containsInput.addEventListener('keypress', (e) => { if (e.key === 'Enter') performSearch() });

async function performSearch() {
    const source = sourceInput.value.trim();
    const contains = containsInput.value.trim();

    if (!source && !contains) {
        alert("Please enter a Source or Contains filter");
        return;
    }

    // UI Loading State
    searchBtn.textContent = "Searching...";
    searchBtn.disabled = true;
    resultsTable.innerHTML = `<tr><td colspan="3" style="text-align:center; padding: 2rem;">Scanning cluster...</td></tr>`;

    try {
        // Build Query URL
        const params = new URLSearchParams();
        if (source) params.append('source', source);
        if (contains) params.append('contains', contains);

        const response = await fetch(`/search?${params.toString()}`);
        if (!response.ok) throw new Error("Search failed");

        const data = await response.json();
        renderResults(data);

    } catch (error) {
        console.error(error);
        resultsTable.innerHTML = `<tr><td colspan="3" style="text-align:center; color: #ef4444;">Error: ${error.message}</td></tr>`;
    } finally {
        searchBtn.textContent = "Search Logs";
        searchBtn.disabled = false;
    }
}

function renderResults(data) {
    // Update Stats
    statEvents.textContent = data.stats.scanned_events.toLocaleString();
    statDuration.textContent = data.stats.duration;
    statMatches.textContent = data.stats.match_count.toLocaleString();
    statsBar.style.display = 'flex';

    // Update Table
    if (data.matches.length === 0) {
        resultsTable.innerHTML = `<tr><td colspan="3" style="text-align:center; padding: 2rem;">No matches found.</td></tr>`;
        return;
    }

    resultsTable.innerHTML = data.matches.map(match => `
        <tr>
            <td>${formatDate(match.timestamp)}</td>
            <td><span style="background: rgba(56, 189, 248, 0.1); color: #38bdf8; padding: 2px 6px; border-radius: 4px;">${match.source}</span></td>
            <td>${match.message}</td>
        </tr>
    `).join('');
}

function formatDate(isoString) {
    const date = new Date(isoString);
    return date.toLocaleString();
}
