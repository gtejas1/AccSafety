const state = {
  mode: 'all',
  season: 'all',
  locations: [],
  selectedId: null,
};

const modeOptions = [
  { id: 'all', label: 'All Modes' },
  { id: 'pedestrian', label: 'Pedestrian' },
  { id: 'bicycle', label: 'Bicycle' },
];

const seasonOptions = [
  { id: 'all', label: 'All Seasons' },
  { id: 'summer', label: 'Summer' },
  { id: 'winter', label: 'Winter' },
  { id: 'shoulder', label: 'Shoulder' },
];

function buildFilterChips(container, options, key) {
  const frag = document.createDocumentFragment();
  options.forEach(({ id, label }) => {
    const chip = document.createElement('button');
    chip.className = 'filter-chip';
    chip.type = 'button';
    chip.textContent = label;
    chip.dataset.value = id;
    chip.setAttribute('aria-pressed', String(state[key] === id));
    chip.addEventListener('click', () => {
      if (state[key] === id) {
        return;
      }
      state[key] = id;
      refreshData();
      updateChipSelection(container, key);
    });
    frag.appendChild(chip);
  });
  container.innerHTML = '';
  container.appendChild(frag);
}

function updateChipSelection(container, key) {
  container.querySelectorAll('.filter-chip').forEach((chip) => {
    const active = chip.dataset.value === state[key];
    chip.setAttribute('aria-pressed', String(active));
  });
}

function renderFilters() {
  const modeGroup = document.getElementById('mode-filter');
  const seasonGroup = document.getElementById('season-filter');
  buildFilterChips(modeGroup, modeOptions, 'mode');
  buildFilterChips(seasonGroup, seasonOptions, 'season');
}

async function refreshData() {
  const params = new URLSearchParams({
    mode: state.mode,
    season: state.season,
  });

  const resultsBody = document.getElementById('results-body');
  const summary = document.getElementById('results-summary');
  const detailContent = document.getElementById('detail-content');

  resultsBody.innerHTML = '<tr class="empty-state"><td colspan="4">Loading locations…</td></tr>';
  summary.textContent = '';
  detailContent.innerHTML = '<p class="empty-state">Select a row to see detailed metrics.</p>';
  state.selectedId = null;

  try {
    const response = await fetch(`/api/explore-data?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Request failed: ${response.status}`);
    }
    const payload = await response.json();
    state.locations = payload.locations || [];
    renderMap(payload.map || {});
    renderTable();
    summary.textContent = payload.summary || '';
  } catch (error) {
    resultsBody.innerHTML = `<tr class="empty-state"><td colspan="4">${error.message}</td></tr>`;
    renderMap({ error: error.message });
  }
}

function renderMap(mapData) {
  const mapContainer = document.getElementById('map-container');
  mapContainer.innerHTML = '';

  if (!state.locations.length) {
    const empty = document.createElement('p');
    empty.className = 'empty-state';
    empty.textContent = 'No locations match the selected filters yet.';
    mapContainer.appendChild(empty);
    return;
  }

  const list = document.createElement('ul');
  list.className = 'map-badges';
  list.style.listStyle = 'none';
  list.style.margin = '0';
  list.style.padding = '0';
  list.style.display = 'grid';
  list.style.gap = '8px';

  state.locations.forEach((location) => {
    const item = document.createElement('li');
    item.style.background = 'rgba(15, 23, 42, 0.08)';
    item.style.borderRadius = '999px';
    item.style.padding = '10px 16px';
    item.style.display = 'flex';
    item.style.alignItems = 'center';
    item.style.justifyContent = 'space-between';
    item.innerHTML = `<span>${location.name}</span><span>${location.averageVolume} avg</span>`;
    list.appendChild(item);
  });

  mapContainer.appendChild(list);

  if (mapData.description) {
    const note = document.createElement('p');
    note.className = 'app-muted';
    note.textContent = mapData.description;
    mapContainer.appendChild(note);
  }
}

function renderTable() {
  const resultsBody = document.getElementById('results-body');

  if (!state.locations.length) {
    resultsBody.innerHTML = '<tr class="empty-state"><td colspan="4">No results for the selected filters.</td></tr>';
    return;
  }

  const fragment = document.createDocumentFragment();

  state.locations.forEach((location) => {
    const row = document.createElement('tr');
    row.tabIndex = 0;
    row.dataset.locationId = location.id;
    if (location.id === state.selectedId) {
      row.classList.add('is-selected');
    }
    row.innerHTML = `
      <td>${location.name}</td>
      <td>${location.modeLabel}</td>
      <td>${location.seasonLabel}</td>
      <td>${location.averageVolume}</td>
    `;

    const activate = () => {
      state.selectedId = location.id;
      renderDetails(location);
      document.querySelectorAll('#results-body tr').forEach((r) => {
        r.classList.toggle('is-selected', r.dataset.locationId === String(state.selectedId));
      });
    };

    row.addEventListener('click', activate);
    row.addEventListener('keydown', (event) => {
      if (event.key === 'Enter' || event.key === ' ') {
        event.preventDefault();
        activate();
      }
    });

    fragment.appendChild(row);
  });

  resultsBody.innerHTML = '';
  resultsBody.appendChild(fragment);
}

function renderDetails(location) {
  const detailContent = document.getElementById('detail-content');
  if (!location) {
    detailContent.innerHTML = '<p class="empty-state">Select a row to see detailed metrics.</p>';
    return;
  }

  detailContent.innerHTML = `
    <h3>${location.name}</h3>
    <p><strong>Mode:</strong> ${location.modeLabel}</p>
    <p><strong>Season:</strong> ${location.seasonLabel}</p>
    <p><strong>Average Volume:</strong> ${location.averageVolume}</p>
    <p class="app-muted">${location.description || 'No additional description provided.'}</p>
  `;
}

function init() {
  renderFilters();
  refreshData();
}

document.addEventListener('DOMContentLoaded', init);
