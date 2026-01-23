const API_BASE = 'http://localhost:8000';

// DOM elements
const searchInput = document.getElementById('searchInput');
const searchBtn = document.getElementById('searchBtn');
const yearFilter = document.getElementById('yearFilter');
const stainFilter = document.getElementById('stainFilter');
const statusDiv = document.getElementById('status');
const resultsDiv = document.getElementById('results');

// Search function
async function searchSlides() {
    const query = searchInput.value.trim();
    const year = yearFilter.value;
    const stain = stainFilter.value;

    if (!query) {
        showStatus('Please enter a search term', 'error');
        return;
    }

    showStatus('Searching...', 'info');

    try {
        let url = `${API_BASE}/search?q=${encodeURIComponent(query)}`;
        if (year) url += `&year=${year}`;
        if (stain) url += `&stain=${stain}`;

        console.log('[DEBUG] Search URL:', url);
        console.log('[DEBUG] Query:', query, '| Year:', year || 'all', '| Stain:', stain || 'all');

        const startTime = performance.now();
        const response = await fetch(url);
        const elapsed = (performance.now() - startTime).toFixed(0);

        console.log(`[DEBUG] Response status: ${response.status} (${elapsed}ms)`);

        if (!response.ok) {
            throw new Error(`HTTP error: ${response.status}`);
        }

        const data = await response.json();
        console.log('[DEBUG] Results:', data.count, 'slides found');
        console.log('[DEBUG] Response data:', data);

        displayResults(data);
    } catch (error) {
        console.error('Search error:', error);
        showStatus(`Error: ${error.message}. Is the backend running?`, 'error');
        resultsDiv.innerHTML = '';
    }
}

// Display search results
function displayResults(data) {
    if (data.count === 0) {
        showStatus(`No results found for "${data.query}"`, 'info');
        resultsDiv.innerHTML = '<div class="no-results">No slides found matching your search.</div>';
        return;
    }

    showStatus(`Found ${data.count} slide(s)`, 'info');

    resultsDiv.innerHTML = data.results.map(slide => `
        <div class="slide-card" data-hash="${slide.slide_hash}">
            <div class="slide-info">
                <h3>${slide.year} - Block ${slide.block_id}</h3>
                <div class="slide-meta">
                    <span>Stain: ${slide.stain_type}</span>
                    <span>ID: ${slide.random_id}</span>
                    ${slide.file_size_bytes ? `<span>Size: ${formatBytes(slide.file_size_bytes)}</span>` : ''}
                </div>
            </div>
            <div class="slide-tags">
                ${(slide.slide_tags || []).map(tag => `<span class="tag">${tag}</span>`).join('')}
                ${(slide.case_tags || []).map(tag => `<span class="tag">${tag}</span>`).join('')}
            </div>
        </div>
    `).join('');
}

// Show status message
function showStatus(message, type) {
    statusDiv.textContent = message;
    statusDiv.className = `status visible ${type}`;
}

// Format bytes to human readable
function formatBytes(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

// Event listeners
searchBtn.addEventListener('click', searchSlides);

searchInput.addEventListener('keypress', (e) => {
    if (e.key === 'Enter') {
        searchSlides();
    }
});

// Check API health on load
async function checkHealth() {
    try {
        const response = await fetch(`${API_BASE}/health`);
        if (response.ok) {
            showStatus('Connected to backend', 'info');
            setTimeout(() => {
                statusDiv.className = 'status';
            }, 2000);
        }
    } catch (error) {
        showStatus('Backend not running. Start it with: cd backend && python -m uvicorn app.main:app --reload', 'error');
    }
}

checkHealth();
