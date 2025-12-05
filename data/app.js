(function () {
  // --- Constants & Config ---
  const MANIFEST_URL = 'manifest.json';
  const DEFAULT_CENTER = { lat: 37.4979, lng: 127.0276 }; // Gangnam
  const DEFAULT_LEVEL = 6;

  // --- State ---
  const state = {
    manifest: [], // [{path, label}, ...]
    loadedData: {}, // { path: [features...] }
    activeDatasets: new Set(), // Set<path>
    filters: {
      housingType: new Set(['아파트', '연립다세대', '단독다가구', '오피스텔']),
      transactionType: new Set(['매매', '전세', '월세'])
    },
    selectedTarget: null // Currently selected building name
  };

  // --- Map & Clusterer ---
  let map = null;
  let clusterer = null;
  let markers = []; // Current markers on map
  let infoWindows = []; // To close open windows

  // --- Initialization ---
  function init() {
    // 1. Init Map
    const container = document.getElementById('map');
    const options = {
      center: new kakao.maps.LatLng(DEFAULT_CENTER.lat, DEFAULT_CENTER.lng),
      level: DEFAULT_LEVEL
    };
    map = new kakao.maps.Map(container, options);

    // 2. Init Clusterer
    clusterer = new kakao.maps.MarkerClusterer({
      map: map,
      averageCenter: true,
      minLevel: 6
    });

    // 3. Load Manifest & Setup UI
    loadManifest();

    // 4. Setup Event Listeners
    setupEventListeners();
  }

  // --- Data Loading ---
  async function loadManifest() {
    const listEl = document.getElementById('dataset-list');
    try {
      const res = await fetch(MANIFEST_URL);
      if (!res.ok) throw new Error('Failed to load manifest');
      state.manifest = await res.json();

      renderDatasetList(state.manifest);
    } catch (e) {
      console.error(e);
      listEl.innerHTML = '<div class="loading-text" style="color:red">데이터 목록 로딩 실패</div>';
    }
  }

  function renderDatasetList(items) {
    const listEl = document.getElementById('dataset-list');
    listEl.innerHTML = '';

    items.forEach((item, index) => {
      // Default select the last item (most recent)
      const isSelected = index === items.length - 1;
      if (isSelected) state.activeDatasets.add(item.path);

      const label = document.createElement('label');
      label.className = 'filter-chip';
      label.innerHTML = `
        <input type="checkbox" name="dataset" value="${item.path}" ${isSelected ? 'checked' : ''}>
        <span class="chip-label">${item.label}</span>
      `;

      label.querySelector('input').addEventListener('change', (e) => {
        if (e.target.checked) {
          state.activeDatasets.add(item.path);
        } else {
          state.activeDatasets.delete(item.path);
        }
        updateMap();
      });

      listEl.appendChild(label);
    });

    // Initial load
    updateMap();
  }

  async function fetchGeoJSON(path) {
    // Fix path for local server: ../data/2025 -> ./2025
    const cleanPath = path.replace('../data/', './');

    if (state.loadedData[cleanPath]) return state.loadedData[cleanPath];

    updateStatus(`데이터 로딩 중... (${cleanPath})`);
    try {
      const res = await fetch(cleanPath);
      const json = await res.json();
      const features = json.features || [];
      state.loadedData[cleanPath] = features;
      return features;
    } catch (e) {
      console.error(`Failed to load ${cleanPath}`, e);
      return [];
    }
  }

  // --- Filtering & Rendering ---
  function isFeatureVisible(f) {
    const p = f.properties || {};

    // Housing Type Filter
    let hType = p['주택유형'] || '기타';
    if (hType.includes('아파트')) hType = '아파트';
    else if (hType.includes('연립') || hType.includes('다세대')) hType = '연립다세대';
    else if (hType.includes('단독') || hType.includes('다가구')) hType = '단독다가구';
    else if (hType.includes('오피스텔')) hType = '오피스텔';

    if (!state.filters.housingType.has(hType)) return false;

    // Transaction Type Filter
    let tType = p['거래유형'] || '기타';
    if (tType === '전월세') {
      const monthlyRent = Number(p['월세'] || 0);
      tType = monthlyRent > 0 ? '월세' : '전세';
    }

    if (!state.filters.transactionType.has(tType)) return false;

    return true;
  }

  async function updateMap() {
    updateStatus('데이터 처리 중...');

    // 1. Gather all features from active datasets
    let allFeatures = [];
    const paths = Array.from(state.activeDatasets);

    for (const path of paths) {
      const features = await fetchGeoJSON(path);
      allFeatures = allFeatures.concat(features);
    }

    // 2. Filter features
    const filtered = allFeatures.filter(isFeatureVisible);

    // 3. Render Markers
    renderMarkers(filtered);
    updateStatus(`표시된 데이터: ${filtered.length.toLocaleString()}건`);

    // 4. Update Data Panel if open
    if (state.selectedTarget) {
      showDataPanel(state.selectedTarget);
    }
  }

  // --- Marker Styling ---
  const MARKER_COLORS = {
    '매매': '#ef4444', // Red
    '전세': '#3b82f6', // Blue
    '월세': '#84cc16', // Lime
    '기타': '#9ca3af'  // Gray
  };

  function getMarkerImage(tType, hType) {
    const color = MARKER_COLORS[tType] || MARKER_COLORS['기타'];
    let svgShape = '';

    // Shape based on Housing Type
    if (hType.includes('아파트')) {
      // Square
      svgShape = `<rect x="4" y="4" width="16" height="16" rx="2" fill="${color}" stroke="#ffffff" stroke-width="2"/>`;
    } else if (hType.includes('단독') || hType.includes('다가구')) {
      // Circle
      svgShape = `<circle cx="12" cy="12" r="9" fill="${color}" stroke="#ffffff" stroke-width="2"/>`;
    } else if (hType.includes('연립') || hType.includes('다세대')) {
      // Triangle
      svgShape = `<polygon points="12,3 22,20 2,20" fill="${color}" stroke="#ffffff" stroke-width="2" stroke-linejoin="round"/>`;
    } else if (hType.includes('오피스텔')) {
      // Diamond
      svgShape = `<polygon points="12,2 22,12 12,22 2,12" fill="${color}" stroke="#ffffff" stroke-width="2" stroke-linejoin="round"/>`;
    } else {
      // Default Circle
      svgShape = `<circle cx="12" cy="12" r="9" fill="${color}" stroke="#ffffff" stroke-width="2"/>`;
    }

    const svg = `
      <svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24">
        ${svgShape}
      </svg>`;

    const svgUrl = 'data:image/svg+xml;charset=utf-8,' + encodeURIComponent(svg);
    const size = new kakao.maps.Size(24, 24);
    return new kakao.maps.MarkerImage(svgUrl, size);
  }

  function renderMarkers(features) {
    // Clear existing
    clusterer.clear();
    markers = [];

    // Create new markers
    const newMarkers = features.map(f => {
      const coords = f.geometry.coordinates; // [lng, lat]
      const latLng = new kakao.maps.LatLng(coords[1], coords[0]);
      const p = f.properties || {};

      // Determine Type
      let tType = p['거래유형'] || '기타';
      if (tType === '전월세') {
        const monthly = Number(p['월세'] || 0);
        tType = monthly > 0 ? '월세' : '전세';
      }

      // Determine Housing Type
      let hType = p['주택유형'] || '기타';

      const marker = new kakao.maps.Marker({
        position: latLng,
        image: getMarkerImage(tType, hType)
      });

      // InfoWindow Event -> Show Data Panel
      kakao.maps.event.addListener(marker, 'click', () => {
        const name = f.properties['단지명/건물명'] || f.properties['건물명'] || f.properties['주소'];
        state.selectedTarget = name; // Track selection
        showDataPanel(name);
      });

      return marker;
    });

    clusterer.addMarkers(newMarkers);
    markers = newMarkers;
  }

  function showInfoWindow(marker, props) {
    // Close others
    infoWindows.forEach(iw => iw.close());
    infoWindows = [];

    const content = createInfoContent(props);
    const iw = new kakao.maps.InfoWindow({
      content: content,
      removable: true
    });

    iw.open(map, marker);
    infoWindows.push(iw);
  }

  function createInfoContent(p) {
    const name = p['단지명/건물명'] || p['건물명'] || p['주소'];
    const price = p['거래금액'] || p['보증금'];
    const monthly = p['월세'] || 0;

    let priceStr = '';
    if (price) {
      const eok = Math.floor(price / 10000);
      const man = price % 10000;
      if (eok > 0) priceStr += `${eok}억 `;
      if (man > 0) priceStr += `${man.toLocaleString()}만원`;
    }

    if (Number(monthly) > 0) {
      priceStr += ` / 월 ${Number(monthly).toLocaleString()}만원`;
    }

    const area = p['전용면적'] ? `${p['전용면적']}㎡` : '-';
    const floor = p['층'] ? `${p['층']}층` : '-';
    const date = p['계약일'] ? `${p['계약년월']}${String(p['계약일']).padStart(2, '0')}` : p['계약년월'];

    // Determine color class based on transaction type
    let tType = p['거래유형'];
    if (tType === '전월세') tType = Number(monthly) > 0 ? '월세' : '전세';

    let colorClass = '';
    if (tType === '매매') colorClass = 'deal-sale';
    else if (tType === '전세') colorClass = 'deal-jeonse';
    else if (tType === '월세') colorClass = 'deal-monthly';

    return `
      <div class="info-window">
        <div class="info-title">${name}</div>
        <div class="info-row">
          <span class="info-label">가격</span>
          <span class="info-val ${colorClass}">${priceStr}</span>
        </div>
        <div class="info-row">
          <span class="info-label">유형</span>
          <span class="info-val">${p['주택유형'] || '-'} (${tType})</span>
        </div>
        <div class="info-row">
          <span class="info-label">면적</span>
          <span class="info-val">${area}</span>
        </div>
        <div class="info-row">
          <span class="info-label">층수</span>
          <span class="info-val">${floor}</span>
        </div>
        <div class="info-row">
          <span class="info-label">계약일</span>
          <span class="info-val">${date}</span>
        </div>
      </div>
    `;
  }

  // --- UI Helpers ---
  function setupEventListeners() {
    // Panel Toggle
    const toggleBtn = document.getElementById('panel-toggle-btn');
    const panel = document.getElementById('control-panel');

    toggleBtn.addEventListener('click', () => {
      panel.classList.toggle('hidden');
      // Update icon based on state
      const isHidden = panel.classList.contains('hidden');
      toggleBtn.innerHTML = isHidden
        ? `<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M4 6h16M4 12h16M4 18h16"></path></svg>`
        : `<svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M18 6L6 18M6 6l12 12"></path></svg>`;
    });

    // Skyview Toggle
    const skyviewBtn = document.getElementById('skyview-btn');
    skyviewBtn.addEventListener('click', () => {
      const currentType = map.getMapTypeId();
      if (currentType === kakao.maps.MapTypeId.ROADMAP) {
        map.setMapTypeId(kakao.maps.MapTypeId.HYBRID);
        skyviewBtn.classList.add('active');
        skyviewBtn.textContent = '지도뷰';
      } else {
        map.setMapTypeId(kakao.maps.MapTypeId.ROADMAP);
        skyviewBtn.classList.remove('active');
        skyviewBtn.textContent = '스카이뷰';
      }
    });

    // Search Autocomplete
    const searchInput = document.getElementById('search-input');
    const searchResults = document.getElementById('search-results');
    let debounceTimer;

    searchInput.addEventListener('input', (e) => {
      const query = e.target.value.trim();
      clearTimeout(debounceTimer);

      if (query.length < 1) {
        searchResults.classList.add('hidden');
        return;
      }

      debounceTimer = setTimeout(() => {
        performSearch(query);
      }, 300);
    });

    // Hide search results when clicking outside
    document.addEventListener('click', (e) => {
      if (!searchInput.contains(e.target) && !searchResults.contains(e.target)) {
        searchResults.classList.add('hidden');
      }
    });

    // Housing Type Checkboxes
    document.querySelectorAll('input[name="housingType"]').forEach(chk => {
      chk.addEventListener('change', (e) => {
        if (e.target.checked) state.filters.housingType.add(e.target.value);
        else state.filters.housingType.delete(e.target.value);
        updateMap();
      });
    });

    // Transaction Type Checkboxes
    document.querySelectorAll('input[name="transactionType"]').forEach(chk => {
      chk.addEventListener('change', (e) => {
        if (e.target.checked) state.filters.transactionType.add(e.target.value);
        else state.filters.transactionType.delete(e.target.value);
        updateMap();
      });
    });
  }

  function performSearch(query) {
    const searchResults = document.getElementById('search-results');

    // Gather all features
    let allFeatures = [];
    Object.values(state.loadedData).forEach(features => {
      allFeatures = allFeatures.concat(features);
    });

    // Filter by name (unique names)
    const uniqueNames = new Set();
    const matches = [];

    for (const f of allFeatures) {
      const p = f.properties || {};
      const name = p['단지명/건물명'] || p['건물명'] || p['주소'] || '';

      if (name.includes(query) && !uniqueNames.has(name)) {
        uniqueNames.add(name);
        matches.push({ name, feature: f });
        if (matches.length >= 10) break; // Limit to 10 results
      }
    }

    if (matches.length === 0) {
      searchResults.innerHTML = '<li class="search-item" style="color:#999; cursor:default">검색 결과가 없습니다</li>';
      searchResults.classList.remove('hidden');
      return;
    }

    searchResults.innerHTML = matches.map(item => {
      // Highlight match
      const regex = new RegExp(`(${query})`, 'gi');
      const highlighted = item.name.replace(regex, '<span class="search-highlight">$1</span>');
      return `<li class="search-item" data-name="${item.name}">${highlighted}</li>`;
    }).join('');

    searchResults.classList.remove('hidden');

    // Add click listeners
    searchResults.querySelectorAll('.search-item').forEach((li, index) => {
      li.addEventListener('click', () => {
        const targetName = matches[index].name;
        const targetFeature = matches[index].feature;

        // Select and Move
        state.selectedTarget = targetName;

        // Pan to location
        const coords = targetFeature.geometry.coordinates;
        const latLng = new kakao.maps.LatLng(coords[1], coords[0]);
        map.panTo(latLng);
        map.setLevel(3); // Zoom in

        // Show Data
        showDataPanel(targetName);

        // Clear search
        document.getElementById('search-input').value = targetName;
        searchResults.classList.add('hidden');
      });
    });
  }

  function updateStatus(msg) {
    const el = document.getElementById('status-bar');
    if (el) el.textContent = msg;
  }

  // --- Data Panel Logic ---
  function showDataPanel(targetName) {
    if (!targetName) return;

    const listEl = document.getElementById('data-list');
    const sectionEl = document.getElementById('data-section');
    const titleEl = document.getElementById('data-title');

    // Show section
    sectionEl.style.display = 'block';
    titleEl.textContent = `${targetName} 거래 내역`;

    // Find all matching features from currently loaded data
    let allFeatures = [];
    Object.values(state.loadedData).forEach(features => {
      allFeatures = allFeatures.concat(features);
    });

    // 1. Filter by name
    let matches = allFeatures.filter(f => {
      const p = f.properties || {};
      const name = p['단지명/건물명'] || p['건물명'] || p['주소'] || '';
      return name.trim() === targetName.trim();
    });

    // 2. Filter by current filters (Housing/Transaction)
    matches = matches.filter(isFeatureVisible);

    // Sort by date (descending)
    matches.sort((a, b) => {
      const pA = a.properties;
      const pB = b.properties;
      const dateA = Number(`${pA['계약년월']}${String(pA['계약일'] || '00').padStart(2, '0')}`);
      const dateB = Number(`${pB['계약년월']}${String(pB['계약일'] || '00').padStart(2, '0')}`);
      return dateB - dateA;
    });

    if (matches.length === 0) {
      listEl.innerHTML = '<div style="padding:20px;text-align:center;color:#666">조건에 맞는 거래 내역이 없습니다.</div>';
      return;
    }

    listEl.innerHTML = matches.map(f => createDataCard(f.properties)).join('');

    // Auto-scroll logic only on first open or explicit user action, 
    // but here we just render. The user can scroll.
  }

  function createDataCard(p) {
    const name = p['단지명/건물명'] || p['건물명'] || p['주소'];
    const price = p['거래금액'] || p['보증금'];
    const monthly = Number(p['월세'] || 0);

    let priceStr = '';
    if (price) {
      const eok = Math.floor(price / 10000);
      const man = price % 10000;
      if (eok > 0) priceStr += `<span style="font-size:1.1em">${eok}억</span> `;
      if (man > 0) priceStr += `${man.toLocaleString()}`;
    } else {
      priceStr = '0';
    }

    // Determine Type & Style
    let tType = p['거래유형'];
    if (tType === '전월세') tType = monthly > 0 ? '월세' : '전세';

    let cardClass = 'card-sale'; // Default
    let badgeText = '매매';

    if (tType === '매매') {
      cardClass = 'card-sale';
      badgeText = '매매';
    } else if (tType === '전세') {
      cardClass = 'card-jeonse';
      badgeText = '전세';
    } else if (tType === '월세') {
      cardClass = 'card-monthly';
      badgeText = '월세';
    }

    const area = p['전용면적'] ? `${p['전용면적']}㎡` : '-';
    // Convert to Pyung (approx)
    const pyung = p['전용면적'] ? (Number(p['전용면적']) / 3.3058).toFixed(1) : '-';

    const floor = p['층'] ? `${p['층']}층` : '-';
    const dong = p['동'] ? `${p['동']}동` : '-';
    const addr = p['주소'] || '-';
    const date = p['계약일'] ? `${String(p['계약년월']).slice(0, 4)}.${String(p['계약년월']).slice(4, 6)}` : '-';

    const depositStr = p['보증금'] ? Number(p['보증금']).toLocaleString() : '0';
    const monthlyStr = monthly ? monthly.toLocaleString() : '0';

    // Conditional Price Detail
    let priceDetail = `(보증금 ${depositStr}만원 / 월세 ${monthlyStr}만원)`;
    if (tType === '매매') {
      priceDetail = ''; // Hide for Sale
    }

    return `
      <div class="data-card ${cardClass}">
        <div class="card-title">${name}</div>
        <div class="card-row">
          유형/거래: ${p['주택유형'] || '-'} / <span class="deal-badge">${badgeText}</span>
        </div>
        <div class="card-row">
          가격: <span class="card-price">${priceStr}</span> ${priceDetail}
        </div>
        <div class="card-row">
          면적: <span style="font-weight:600;color:#2563eb">${pyung}평</span>
        </div>
        <div class="card-row">
          동/층: ${dong} / ${floor}
        </div>
        <div class="card-row">
          주소: ${addr}
        </div>
        <div class="card-row" style="color:#6b7280; margin-top:4px">
          계약: ${date}
        </div>
      </div>
    `;
  }

  // --- Boot ---
  // Wait for Kakao SDK to be ready
  const checkKakao = setInterval(() => {
    if (window.kakao && window.kakao.maps) {
      clearInterval(checkKakao);
      kakao.maps.load(init);
    }
  }, 100);

})();