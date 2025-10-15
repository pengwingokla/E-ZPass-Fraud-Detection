// MockPass Website JavaScript - Simplified for Statements Only

// --- INITIALIZATION ---
document.addEventListener('DOMContentLoaded', function() {
    // Ensure statements section is visible
    const statementsSection = document.getElementById('statements-section');
    if (statementsSection) {
        statementsSection.classList.remove('hidden');
    }
    
    console.log("MockPass Statements Page Initialized.");
});

// --- FILE DOWNLOADS ---
function downloadFile(filename) {
    alert(`The feature to download "${filename}" is not yet implemented.`);
}

function parseCSV(csvText) {
    const lines = csvText.trim().split('\n');
    if (lines.length < 2) return [];

    const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
    const transactions = [];

    // Find the indices of the columns we need, accounting for variations
    const dateIndex = headers.indexOf('TRANSACTION DATE');
    const tagIndex = headers.indexOf('TAG/PLATE NUMBER');
    const agencyIndex = headers.indexOf('AGENCY');
    const amountIndex = headers.indexOf('AMOUNT');
    const entryPlazaIndex = headers.indexOf('ENTRY PLAZA');
    const exitPlazaIndex = headers.indexOf('EXIT PLAZA');
    const descriptionIndex = headers.indexOf('DESCRIPTION'); // Fallback

    for (let i = 1; i < lines.length; i++) {
        const data = lines[i].split(',');
        
        if (data.length < headers.length) continue;

        // Extract date and time
        const dateValue = data[dateIndex];
        const dateParts = dateValue.match(/(\d{1,2})\/(\d{1,2})\/(\d{4})/);
        if (!dateParts) continue; // Skip if date format is not as expected
        
        const month = dateParts[1].padStart(2, '0');
        const day = dateParts[2].padStart(2, '0');
        const year = dateParts[3];
        const formattedDate = `${year}-${month}-${day}`;

        const timeMatch = dateValue.match(/(\d{2}:\d{2}:\d{2}\s[AP]M)/);
        const time = timeMatch ? timeMatch[0] : (data[headers.indexOf('EXIT TIME')] || '');

        // Construct location string
        let location = '';
        const entryPlaza = data[entryPlazaIndex] ? data[entryPlazaIndex].trim() : '';
        const exitPlaza = data[exitPlazaIndex] ? data[exitPlazaIndex].trim() : '';

        if (entryPlaza && exitPlaza && entryPlaza !== '-') {
            location = `${entryPlaza} to ${exitPlaza}`;
        } else if (exitPlaza && exitPlaza !== '-') {
            location = exitPlaza;
        } else {
            location = data[descriptionIndex] ? data[descriptionIndex].trim() : 'N/A';
        }

        const amountStr = data[amountIndex].replace(/[()$"]/g, '').trim();
        const amount = parseFloat(amountStr);

        transactions.push({
            date: formattedDate,
            time: time,
            tag: data[tagIndex].trim(),
            agency: data[agencyIndex].trim(),
            location: location,
            amount: isNaN(amount) ? 0 : amount,
            status: 'Completed' // Default status
        });
    }
    return transactions;
}

// --- UI NAVIGATION ---
function showSection(sectionName) {
    // Hide all sections
    document.querySelectorAll('.section').forEach(section => {
        section.classList.add('hidden');
    });

    // Show the target section
    const targetSection = document.getElementById(sectionName + '-section');
    if (targetSection) {
        targetSection.classList.remove('hidden');
    }

    // Update navigation link styles
    document.querySelectorAll('.nav-link').forEach(link => {
        link.classList.remove('active');
        // Check if the link's onclick corresponds to the sectionName
        if (link.getAttribute('onclick') === `showSection('${sectionName}')`) {
            link.classList.add('active');
        }
    });
}


// --- DYNAMIC CONTENT POPULATION ---
function populateAccountSummary() {
    const balanceEl = document.getElementById('current-balance');
    // NOTE: The balance is a static value as it's not available in the provided transaction data.
    if (balanceEl) {
        balanceEl.textContent = '$8,163.81';
    }
}

function populateVehicleList() {
    const vehicleListEl = document.getElementById('vehicle-list');
    if (!vehicleListEl) return;

    // Get unique vehicle tags from the data
    const uniqueTags = [...new Set(allTransactions.map(t => t.tag))];
    
    vehicleListEl.innerHTML = ''; // Clear existing list
    uniqueTags.forEach(tag => {
        const vehicleType = tag.startsWith('SG') ? 'Commercial Vehicle' : 'Passenger Vehicle';
        const item = document.createElement('div');
        item.className = 'vehicle-item';
        item.innerHTML = `
            <span class="vehicle-tag">Tag: ${tag}</span>
            <span class="vehicle-type">${vehicleType}</span>
        `;
        vehicleListEl.appendChild(item);
    });
}


// --- TRANSACTION FILTERING & DISPLAY ---
function filterTransactions() {
    const selectedPeriod = document.getElementById('date-filter').value;
    const tbody = document.getElementById('transactions-tbody');
    if (!tbody) return;

    let filteredTransactions = [];

    if (selectedPeriod === 'all') {
        filteredTransactions = allTransactions;
    } else if (selectedPeriod === 'may') {
        filteredTransactions = allTransactions.filter(t => t.date.startsWith('2025-05-'));
    } else if (selectedPeriod === 'april') {
        filteredTransactions = allTransactions.filter(t => t.date.startsWith('2025-04-'));
    } else {
        // For June, July, August, etc., the list will be empty
        filteredTransactions = [];
    }

    renderTransactions(filteredTransactions, tbody);
}

function renderTransactions(transactions, tbody) {
    tbody.innerHTML = ''; // Clear table

    if (transactions.length === 0) {
        const row = document.createElement('tr');
        row.innerHTML = `
            <td colspan="7" class="text-center" style="padding: 2rem; color: #666;">
                No transactions found for the selected period.
            </td>
        `;
        tbody.appendChild(row);
        return;
    }

    transactions.forEach(t => {
        const row = document.createElement('tr');
        const statusClass = `status-${t.status.toLowerCase()}`;
        row.innerHTML = `
            <td>${t.date}</td>
            <td>${t.time}</td>
            <td>${t.tag}</td>
            <td>${t.agency}</td>
            <td>${t.location}</td>
            <td>$${t.amount.toFixed(2)}</td>
            <td><span class="${statusClass}">${t.status}</span></td>
        `;
        tbody.appendChild(row);
    });
}


// --- FILE DOWNLOADS ---
function downloadFile(filename) {
    // This function is now primarily for features not yet implemented.
    alert(`The feature to download "${filename}" is not yet implemented.`);
}

// --- CONSOLE WELCOME ---
console.log("MockPass Dashboard Initialized. Loading data dynamically from CSVs.");